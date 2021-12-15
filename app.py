from flask import Flask,jsonify,request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
import json
import seaborn as sns
from flask_cors import CORS
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres@localhost:5432/postgres'

db = SQLAlchemy(app)

cors = CORS(app, resources={r"*": {"origins": "*"}})

@app.route("/")
def root():
    return "<h1 style='color:blue'>Hello There!</h1>"

@app.route("/routes")
def routes():
    result = db.session.execute('select route_long_name from routes;')
    result  = list(dict(i) for i in result.all())
    return jsonify(json_list = result)


@app.route("/stops")
def stops():
    
    route = request.args.get("route")
    
    stops_query = text("Select DISTINCT(stop_name,stop_sequence),stop_name,stop_sequence from route_stop_mapping  where route_long_name  =  :route order by stop_sequence;  ")

    stops  = db.session.execute(stops_query,{'route':route})
    
    stop_names  = [i.stop_name for i in stops]
    
    return jsonify(stops = stop_names)

@app.route("/chartdata")
def chart_data():
    route  =request.args.get("route")
    text1= text("Select user_start_stop_name,SUM(ticket_count) from tickets where route_long_name =  :route group by user_start_stop_name;")
    text2= text("Select user_end_stop_name,SUM(ticket_count) from tickets where route_long_name =  :route group by user_end_stop_name;")
    stops_query = text("Select DISTINCT(stop_name,stop_sequence),stop_name,stop_sequence from route_stop_mapping  where route_long_name  =  :route order by stop_sequence;  ")
    dic_onboarding = {}
    dic_eliding = {}
    
    
    
    onboarding = db.session.execute(text1,{'route':route})
    eliding = db.session.execute(text2,{'route' :route})
    stops  = db.session.execute(stops_query,{'route':route})
    #stops  = list(dict(i) for i in stops.all())
    for i in onboarding:
        dic_onboarding[i.user_start_stop_name] = i.sum;
        
    for i in eliding:
        dic_eliding[i.user_end_stop_name] = i.sum;
        
    
    stop_arr = []
    onboarding_arr = []
    offloading_arr = []
    for i in stops:
        on_count = el_count=0;
        if i.stop_name in dic_onboarding.keys():
            on_count = dic_onboarding[i.stop_name]
        if i.stop_name in dic_eliding.keys():
            el_count = dic_eliding[i.stop_name]
        
        stop_arr.append(i.stop_name)
        onboarding_arr.append(on_count)
        offloading_arr.append(el_count)   
    
    
    
    return jsonify(stop_arr,onboarding_arr,offloading_arr)


@app.route("/stopbarchart")
def stop_bar_data():
    # need to get data, 
    
    stop   = request.args.get("stop")
    
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    text1= text("Select route_long_name,user_start_stop_name,user_end_stop_name,SUM(ticket_count) from tickets where (user_start_stop_name = :stop or user_end_stop_name = :stop)  group by user_start_stop_name,user_end_stop_name,route_long_name order by route_long_name; ")
    
    onboarding_stops = []
    eliding_stops = []
    onboarding_data = []
    eliding_data = []
    route_dic_onboarding = {}
    route_dic_eliding = {}
    onboarding_stop_set = set()
    eliding_stop_set = set()
    result  = db.session.execute(text1,{'stop':stop})
    #iterate,populate route dic with tuple of stop,onboarding and eliding.
     
    # collect layer for all s
    for row in result:
        if row.user_start_stop_name == stop:
            eliding_stop_set.add(row.user_end_stop_name)
            if row.route_long_name not in route_dic_eliding.keys():
                route_dic_eliding[row.route_long_name] = {row.user_end_stop_name: row.sum}
            else:
                route_dic_eliding[row.route_long_name][row.user_end_stop_name]=row.sum
        else:    
            onboarding_stop_set.add(row.user_start_stop_name)
            if row.route_long_name not in route_dic_onboarding.keys():
                route_dic_onboarding[row.route_long_name] = {row.user_start_stop_name:row.sum}
            else:
                route_dic_onboarding[row.route_long_name][row.user_start_stop_name]=row.sum
    
    onboarding_dic = {'labels':list(onboarding_stop_set),
                        'datasets':[]
                      }
    eliding_dic ={'labels':list(eliding_stop_set),
                        'datasets':[]
                      }
    color_count = len(route_dic_onboarding.keys())
    pallete = sns.color_palette(None,color_count)
    for j,color in zip(route_dic_onboarding.keys(),pallete.as_hex()):
        onboarding_dic['datasets'] += [{'backgroundColor':color,'stack':1,'label':j,'data':[int(route_dic_onboarding[j][i]) if i in route_dic_onboarding[j].keys() else 0 for i in onboarding_stop_set]}]
    color_count = len(route_dic_eliding.keys())
    pallete = sns.color_palette(None,color_count)
    for j,color in zip(route_dic_eliding.keys(),pallete.as_hex()):
        eliding_dic['datasets'] += [{'backgroundColor':color,'stack':1,'label':j,'data':[int(route_dic_eliding[j][i]) if i in route_dic_eliding[j].keys() else 0 for i in eliding_stop_set]}]
    
        
    # prepare json arrays as needed by chartjs
    
    
   
    final_dict = {'arrivals':onboarding_dic,'departures':eliding_dic}    
    return jsonify(final_dict)


@app.route("/stopsunburst")
def stop_sun_data():
    # need to get data, 
    
    stop   = request.args.get("stop")
    
    # query database for tuple stats <stop,route,onboarding/eliding/count>
    #prepare onboarding/eliding array and prepare toggle on frontend
    
    text1= text("Select route_long_name,user_start_stop_name,user_end_stop_name,SUM(ticket_count) from tickets where (user_start_stop_name = :stop or user_end_stop_name = :stop)  group by user_start_stop_name,user_end_stop_name,route_long_name order by route_long_name; ")
    
    onboarding_dic = {}
    eliding_dic = {}
    onboarding_route_set = set()
    eliding_route_set = set()
    result  = db.session.execute(text1,{'stop':stop})
    #iterate,populate route dic with tuple of stop,onboarding and eliding.
    onboarding_return_dic = {'name':'Onboarding',
                        'children':[]
                      }
    eliding_return_dic ={'name':'Eliding',
                        'children':[]
                      }
    for row in result:
        if row.user_start_stop_name == stop:
            onboarding_route_set.add(row.route_long_name)
            if (row.route_long_name in onboarding_dic.keys()):
                    onboarding_dic[row.route_long_name]+=1
            else:
                    onboarding_dic[row.route_long_name]=1
        else:    
            eliding_route_set.add(row.route_long_name)
            if (row.route_long_name in eliding_dic.keys()):
                    eliding_dic[row.route_long_name]+=1
            else:
                    eliding_dic[row.route_long_name]=1
  
    onboarding_return_dic['children'] = [{'name':i,'value':onboarding_dic[i]} for i in onboarding_route_set]
    eliding_return_dic['children'] = [{'name':i,'value':eliding_dic[i]} for i in eliding_route_set]
        
        
   
    final_dict ={'name':'root','children':[onboarding_return_dic,eliding_return_dic]}   
    return jsonify(final_dict)

