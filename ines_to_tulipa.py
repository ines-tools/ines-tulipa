import spinedb_api as api
from spinedb_api import DatabaseMapping, DateTime, Map, to_database
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
import yaml
import sys
from ines_tools import ines_transform
import pandas as pd
import json
import numpy as np

def nested_index_names(value, names = None, depth = 0):
    if names is None:
        names = []
    if depth == len(names):
        names.append(value.index_name)
    elif value.index_name != names[-1]:
        raise RuntimeError(f"Index names at depth {depth} do no match: {value.index_name} vs. {names[-1]}")
    for y in value.values:
        if isinstance(y, IndexedValue):
            nested_index_names(y, names, depth + 1)
    return names

operations = {
    "multiply": lambda x, y: x * y,
    "add": lambda x, y: x + y,
    "subtract": lambda x, y: x - y,
    "divide": lambda x, y: x / y,
    "constant": lambda x, y: y
}

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")

with open('ines_to_tulipa_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('ines_to_tulipa_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open('ines_to_tulipa_methods.yaml', 'r') as file:
    parameter_methods = yaml.safe_load(file)
with open('ines_to_tulipa_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
with open('settings.yaml', 'r') as file:
    settings = yaml.safe_load(file)

def add_entity_group(db_map : DatabaseMapping, class_name : str, group : str, member : str) -> None:
    _, error = db_map.add_entity_group_item(group_name = group, member_name = member, entity_class_name=class_name)
    if error is not None:
        raise RuntimeError(error)
    
def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)
    
def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)

def add_scenario_alternative(db_map : DatabaseMapping,name_scenario : str, name_alternative : str, rank_int = None) -> None:
    _, error = db_map.add_scenario_alternative_item(scenario_name = name_scenario, alternative_name = name_alternative, rank = rank_int)
    if error is not None:
        raise RuntimeError(error)
    
def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out) as target_db:
            ## Empty the database
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            target_db.purge_items('scenario')
            target_db.refresh_session()
            target_db.commit_session("Purged stuff")

            ## Copy alternatives
            for alternative in source_db.get_alternative_items():
                target_db.add_alternative_item(name=alternative["name"])
            for scenario in source_db.get_scenario_items():
                target_db.add_scenario_item(name=scenario["name"])
            for scenario_alternative in source_db.get_scenario_alternative_items():
                target_db.add_scenario_alternative_item(alternative_name=scenario_alternative["alternative_name"],
                                                        scenario_name=scenario_alternative["scenario_name"],
                                                        rank=scenario_alternative["rank"])

            # creating main entities
            print("adding periods")
            add_periods(source_db, target_db)
            print("adding entities")
            add_entities(source_db, target_db)
            print("adding capacities")
            add_capacity(source_db,target_db)
            print("adding existing units")
            add_existing_units(source_db,target_db)
            print("adding investment and retirement methods")
            add_investable_decommisionable(source_db,target_db)
            print("adding fixed units")
            add_fixed_units(source_db,target_db)
            print("adding flow relationships")
            add_flow_relationships(source_db,target_db)
            print("adding costs")
            add_costs(source_db,target_db)
            print("adding emissions")
            add_emissions(source_db,target_db)
            print("adding profiles")
            add_profiles(source_db,target_db)
            

def add_periods(source_db,target_db):

    duration      = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])["data"]
    periods       = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]
    resolution    = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
    
    steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
    for period in periods:
        add_entity(target_db,"year",(period[1:],))
        add_parameter_value(target_db,"year","is_milestone","Base",(period[1:],),True)
        add_parameter_value(target_db,"year","length","Base",(period[1:],),steps)
        add_parameter_value(target_db,"year","timeframe_data","Base",(period[1:],),{"type":"map","index_type":"str","index_name":"period","data":{1:steps}})
        add_entity(target_db,"commission",(period[1:],))
    
    try:
        target_db.commit_session("Added periods")
    except:
        print("commit adding periods error")

def add_entities(source_db,target_db):

    storages = []
    for entity in [entity_item for entity_item in source_db.get_entity_items(entity_class_name = "node")]:

        add_entity(target_db,"asset",(entity["name"],))
        node_type = source_db.get_parameter_value_item(entity_class_name = "node", entity_byname = (entity["name"],), alternative_name ="Base", parameter_definition_name = "node_type")
        if node_type:
            if node_type["parsed_value"] == "storage":
                storages.append(entity["name"])
                add_parameter_value(target_db,"asset","type","Base",(entity["name"],),"storage")
            else:
                add_parameter_value(target_db,"asset","type","Base",(entity["name"],),"hub")
            
    for entity in [entity_item for entity_item in source_db.get_entity_items(entity_class_name = "link")]:
        node1_node2 = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2]) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if entity["name"] == entity_link["entity_byname"][1]]
        for node_1, node_2 in node1_node2:
            add_entity(target_db,"asset__asset",(node_1,node_2))
            add_parameter_value(target_db,"asset__asset","is_transport","Base",(node_1,node_2),True)
        
    for entity in [entity_item for entity_item in source_db.get_entity_items(entity_class_name = "unit")]:
        nodes1 = [entity_from["entity_byname"][0] for entity_from in source_db.get_entity_items(entity_class_name = "node__to_unit") if entity["name"] == entity_from["entity_byname"][1]]
        nodes2 = [entity_to["entity_byname"][1] for entity_to in source_db.get_entity_items(entity_class_name = "unit__to_node") if entity["name"] == entity_to["entity_byname"][0]]

        if not nodes1:
            add_entity(target_db,"asset",(entity["name"],))
            add_parameter_value(target_db,"asset","type","Base",(entity["name"],),"producer")
            for node2 in nodes2:
                add_entity(target_db,"asset__asset",(entity["name"],node2))
        else:
            add_entity(target_db,"asset",(entity["name"],))
            for node1 in nodes1:
                add_entity(target_db,"asset__asset",(node1,entity["name"]))
            for node2 in nodes2:
                add_entity(target_db,"asset__asset",(entity["name"],node2))

    try:
        target_db.commit_session("Added entities")
    except:
        print("commit adding entities error")
    
    return storages

def add_capacity(source_db,target_db):

    units_cap = {entity_item["name"]:{} for entity_item in source_db.get_entity_items(entity_class_name = "unit")}
    for storage_capacity in source_db.get_parameter_value_items(parameter_definition_name = "storage_capacity"):
        add_parameter_value(target_db,"asset","capacity_storage_energy",storage_capacity["alternative_name"],storage_capacity["entity_byname"],storage_capacity["parsed_value"])
        add_parameter_value(target_db,"asset","capacity",storage_capacity["alternative_name"],storage_capacity["entity_byname"],storage_capacity["parsed_value"])
        add_parameter_value(target_db,"asset","storage_method_energy",storage_capacity["alternative_name"],storage_capacity["entity_byname"],True)
    
    for entity_capacity in source_db.get_parameter_value_items(parameter_definition_name = "capacity"):

        if entity_capacity["entity_class_name"] == "link":
            node1,node2 = [(entity_item["entity_byname"][0],entity_item["entity_bynname"][2]) for entity_item in source_db.get_entity_items(entity_class_name = "node__link__node") if entity_item["entity_byname"][1]==entity_capacity["entity_byname"][0]][0]
            add_parameter_value(target_db,"asset__asset","capacity",entity_capacity["alternative_name"],(node1,node2),entity_capacity["parsed_value"])
            if source_db.get_entity_item(entity_class_name = "node__link__node",entity_byname = (node2,entity_capacity["entity_byname"][0],node1)):
                add_parameter_value(target_db,"asset__asset","capacity",entity_capacity["alternative_name"],(node2,node1),entity_capacity["parsed_value"])
            
        elif entity_capacity["entity_class_name"] == "node__link__node":
            node1 = entity_capacity["entity_byname"][0]
            node2 = entity_capacity["entity_byname"][2]
            add_parameter_value(target_db,"asset__asset","capacity",entity_capacity["alternative_name"],(node1,node2),entity_capacity["parsed_value"])

        elif entity_capacity["entity_class_name"] == "unit__to_node":
            units_cap[entity_capacity["entity_byname"][0]][entity_capacity["entity_byname"][1]] = ["to",entity_capacity["parsed_value"]]

        elif entity_capacity["entity_class_name"] == "node__to_unit":
            from_capacity = entity_capacity["parsed_value"]
            efficiency = [entity_p for entity_p in source_db.get_parameter_value_items(entity_class_name = "unit_flow__unit_flow",alternative_name = "Base",parameter_definition_name = "equality_ratio") if entity_p["entity_byname"][2] == entity_capacity["entity_byname"][0] and entity_p["entity_byname"][3] == entity_capacity["entity_byname"][1]][0]
            new_to_node = efficiency["entity_byname"][1]
            if efficiency["type"] == "float":
                param_value = efficiency["parsed_value"]*from_capacity
            elif efficiency["type"] == "map":
                param_value = dict(zip([i[1:] for i in efficiency["parsed_value"].indexes],[i*from_capacity for i in efficiency["parsed_value"].values]))
            units_cap[entity_capacity["entity_byname"][1]][new_to_node] = ["from",param_value]

    for unit in units_cap:
        if len(units_cap[unit]) == 1:
            for node in units_cap[unit]:
                if not isinstance(units_cap[unit][node][1],dict): 
                    add_parameter_value(target_db,"asset","capacity","Base",(unit,),units_cap[unit][node][1])
                else:
                    exit("need to implement a capability for different capacities in different comission years for unit", unit)
        else:
            to_condition = False
            for node in units_cap[unit]:
                if units_cap[unit][node][0] == "to":
                    unit_capacity = units_cap[unit][node][1]
                    to_condition = True
                    add_parameter_value(target_db,"asset","capacity","Base",(unit,),unit_capacity)

            if to_condition:
                for node in units_cap[unit]:
                    if units_cap[unit][node][0] == "from":
                        for commission_year in target_db.get_entity_items(entity_class_name = "commission"):
                            try:
                                add_entity(target_db,"asset__asset__commission",(unit,node,commission_year["name"]))
                            except:
                                pass
                            if isinstance(units_cap[unit][node][1],dict):
                                add_parameter_value(target_db,"asset__asset__commission","capacity_coefficient","Base",(unit,node,commission_year["name"]),unit_capacity/units_cap[unit][node][1][commission_year["name"]])
                            else:
                                add_parameter_value(target_db,"asset__asset__commission","capacity_coefficient","Base",(unit,node,commission_year["name"]),unit_capacity/units_cap[unit][node][1])
            else:
                exit("need to implement a capability for different capacities in different comission years and multiple node__to_unit flows for unit",unit)

    # Filters apply: No capacity, then capacity_coefficient = 0
    years = [year["name"] for year in target_db.get_entity_items(entity_class_name = "commission")]
    for type_item in target_db.get_parameter_value_items(entity_class_name = "asset", parameter_definition_name = "type"):
        if type_item["parsed_value"] in ["producer","conversion"]:
            capacity_param = target_db.get_parameter_value_items(entity_class_name = "asset", entity_byname = type_item["entity_byname"], parameter_definition_name = "capacity")
            if not capacity_param:
                asset_flows = [entity_i["entity_byname"][1] for entity_i in target_db.get_entity_items(entity_class_name = "asset__asset") if entity_i["entity_byname"][0] == type_item["name"]]
                if asset_flows:
                    for asset_out in asset_flows:
                        for year in years:
                            entity_target = (type_item["name"],asset_out,year)
                            entity_class_target  = "asset__asset__commission"
                            try:
                                add_entity(target_db,entity_class_target,entity_target)
                            except:
                                pass
                            add_parameter_value(target_db,entity_class_target,"capacity_coefficient","Base",entity_target,0.0)
    
    for type_item in target_db.get_parameter_value_items(entity_class_name = "asset__asset", parameter_definition_name = "is_transport"):
        if type_item["parsed_value"] == True:
            capacity_param = target_db.get_parameter_value_items(entity_class_name = "asset__asset", entity_byname = type_item["entity_byname"], parameter_definition_name = "capacity")
            if not capacity_param:
                for year in years:
                    entity_target = (type_item["entity_byname"][0],type_item["entity_byname"][1],year)
                    entity_class_target  = "asset__asset__commission"
                    try:
                        add_entity(target_db,entity_class_target,entity_target)
                    except:
                        pass
                    add_parameter_value(target_db,entity_class_target,"capacity_coefficient","Base",entity_target,0.0)
        
    try:
        target_db.commit_session("Added capacities")
    except:
        print("commit adding capacities error")

def add_existing_units(source_db,target_db):

    # units and storages and links
    existing_name = {"unit":"units_existing","node":"storages_existing","link":"links_existing"}
    target_param = {"unit":"initial_units","node":"initial_storage_units","link":"initial_export_units"}
    years = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]

    for entity_class in ["unit","node","link"]:
        existing_parameters = source_db.get_parameter_value_items(entity_class_name = entity_class, parameter_definition_name = existing_name[entity_class])
        for existing_parameter in existing_parameters:
            for year in years:
                if entity_class != "link":
                    entity_class_target = "asset__commission__year"
                    entity_bynames = [(existing_parameter["entity_byname"][0],min(years),year)]
                else:
                    entity_class_target = "asset__asset__commission__year"
                    entity_bynames = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],min(years),year) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if existing_parameter["entity_byname"][0] == entity_link["entity_byname"][1]]
                for entity_byname in entity_bynames:
                    try:
                        add_entity(target_db,entity_class_target,entity_byname)
                    except:
                        print("not added",entity_class_target,entity_byname)
                        pass
                    if existing_parameter["type"] == "map":
                        existing_dict = dict(zip([i[1:] for i in existing_parameter["parsed_value"].indexes],existing_parameter["parsed_value"].values))
                        if len(existing_dict) > 1:
                            cap_value = existing_dict[year]
                        else:
                            cap_value = existing_dict[min(years)]
                    elif  existing_parameter["type"] == "float": 
                        cap_value = existing_parameter["parsed_value"]

                    add_parameter_value(target_db,entity_class_target,target_param[entity_class],existing_parameter["alternative_name"],entity_byname,cap_value)
                
    try:
        target_db.commit_session("Added existing units")
    except:
        print("commit adding existing units error")

def add_investable_decommisionable(source_db,target_db):

    years   = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    years_c = [year["name"] for year in target_db.get_entity_items(entity_class_name = "commission")]

    investment_method = {"unit":"investment_method","node":"storage_investment_method","link":"investment_method"}
    retirement_method = {"unit":"retirement_method","node":"storage_retirement_method","link":"retirement_method"}

    target_decommissionable = {"unit":"asset__commission__year","node":"asset__commission__year","link":"asset__asset__commission__year"}
    target_investable       = {"unit":"asset__year","node":"asset__year","link":"asset__asset__year"}

    for entity_class in ["unit","node","link"]:
        for entity_item in source_db.get_entity_items(entity_class_name = entity_class):
            
            #global condition: having capacity
            if entity_class != "link":
                original_bynames = [(entity_item["entity_byname"][0],)]
                for original_byname in original_bynames:
                    if target_db.get_parameter_value_item(entity_class_name = "asset", parameter_definition_name = "capacity", entity_byname = original_byname, alternative_name = "Base"):
                        global_condition = True
                    else:
                        global_condition = False
            else:
                original_bynames = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2]) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if entity_item["name"] == entity_link["entity_byname"][1]]
                for original_byname in original_bynames:
                    is_transport_cond = target_db.get_parameter_value_item(entity_class_name = "asset__asset", parameter_definition_name = "is_transport", entity_byname = original_byname, alternative_name = "Base")
                    if is_transport_cond:
                        if is_transport_cond["parsed_value"] and target_db.get_parameter_value_item(entity_class_name = "asset__asset", parameter_definition_name = "capacity", entity_byname = original_byname, alternative_name = "Base"):
                            global_condition = True
                        else:
                            global_condition = False
                    else:
                        global_condition = False

            
            # is decommisionable?
            retirement_value_ = source_db.get_parameter_value_item(entity_class_name = entity_class, parameter_definition_name = retirement_method[entity_class], entity_byname = entity_item["entity_byname"], alternative_name = "Base")
            if not retirement_value_:
                decommission_condition = True 
            else: 
                decommission_condition = True if retirement_value_["parsed_value"] != "not_retired" else False
            
            if decommission_condition and global_condition:
                for year_c in years_c:
                    for year in years:
                    
                        if year >= year_c:
                            if entity_class != "link":
                                entity_bynames = [(entity_item["entity_byname"][0],year_c,year)]
                            else:
                                entity_bynames = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],year_c,year) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if entity_item["name"] == entity_link["entity_byname"][1]]

                            for entity_byname in entity_bynames:
                                try:
                                    add_entity(target_db,target_decommissionable[entity_class],entity_byname)
                                except:
                                    pass
                                add_parameter_value(target_db,target_decommissionable[entity_class],"decommissionable","Base",entity_byname,decommission_condition)
            
            # is investable?
            investment_value_ = source_db.get_parameter_value_item(entity_class_name = entity_class, parameter_definition_name = investment_method[entity_class], entity_byname = entity_item["entity_byname"], alternative_name = "Base")
            investment_condition = False if not investment_value_ else (True if investment_value_["parsed_value"] != "not_allowed" else False)
            if investment_condition and global_condition:
                for year in years:
                    if entity_class != "link":
                        entity_bynames = [(entity_item["entity_byname"][0],year)]
                    else:
                        entity_bynames = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],year) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if entity_item["name"] == entity_link["entity_byname"][1]]

                    for entity_byname in entity_bynames:
                        try:    
                            add_entity(target_db,target_investable[entity_class],entity_byname)
                        except:
                            pass
                        add_parameter_value(target_db,target_investable[entity_class],"investable","Base",entity_byname,investment_condition)
    
    # It is decommissionable every year, the new units
    
    try:
        target_db.commit_session("Added ables")
    except:
        print("commit adding ables error")

def add_fixed_units(source_db,target_db):

    # units and storages and links
    existing_name = {"unit":"units_fix_cumulative","node":"storages_fix_cumulative","link":"links_fix_cumulative"}
    target_param = {"unit":"initial_units","node":"initial_storage_units","link":"initial_export_units"}
    years = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    if_decommissionable = target_db.get_parameter_value_items(parameter_definition_name = "decommissionable")
    if_investable = target_db.get_parameter_value_items(parameter_definition_name = "investable")
    
    for entity_class in ["unit","node","link"]:
        existing_parameters = source_db.get_parameter_value_items(entity_class_name = entity_class, parameter_definition_name = existing_name[entity_class])
        for existing_parameter in existing_parameters:
            for year in years:
                if entity_class != "link":
                    entity_class_target = "asset__commission__year"
                    entity_bynames = [(existing_parameter["entity_byname"][0],year,year)]
                else:
                    entity_class_target = "asset__asset__commission__year"
                    entity_bynames = [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],year,year) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if existing_parameter["entity_byname"][0] == entity_link["entity_byname"][1]]
                for entity_byname in entity_bynames:
                    try:
                        add_entity(target_db,entity_class_target,entity_byname)
                    except:
                        print("not added",entity_class_target,entity_byname)
                        pass
                    if existing_parameter["type"] == "map":
                        existing_dict = dict(zip([i[1:] for i in existing_parameter["parsed_value"].indexes],existing_parameter["parsed_value"].values))
                        cap_value = existing_dict[year]
                    elif  existing_parameter["type"] == "float": 
                        cap_value = existing_parameter["parsed_value"]

                    add_parameter_value(target_db,entity_class_target,target_param[entity_class],existing_parameter["alternative_name"],entity_byname,cap_value)
                
                    target_comparison = (existing_parameter["entity_byname"][0],) if entity_class != "link" else (existing_parameter["entity_byname"][0],existing_parameter["entity_byname"][1])
                    limit_list = 1  if entity_class != "link" else 2
                    try:
                        for decom_item in [i for i in if_decommissionable if i["entity_byname"][:limit_list] == target_comparison]:
                            target_db.remove_item("parameter_value",decom_item["id"])
                    except:
                        pass

                    target_comparison = (existing_parameter["entity_byname"][0],) if entity_class != "link" else (existing_parameter["entity_byname"][0],existing_parameter["entity_byname"][1])
                    limit_list = 1  if entity_class != "link" else 2
                    try:
                        for inves_item in [i for i in if_investable if i["entity_byname"][:limit_list] == target_comparison]:
                            target_db.remove_item("parameter_value",inves_item["id"])
                    except:
                        pass
                    
    try:
        target_db.commit_session("Added fixed units")
    except:
        print("commit adding fixed units error")

def add_flow_relationships(source_db,target_db):

    years  = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    yearsc = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    starttime = {} 
    year_repr = {}
    for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
        starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
        year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]

    duration      = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])["data"]
    starttime_sp  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
    resolution    = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]
    
    for parameter_name in ["equality_ratio"]:
        for parameter_dict in source_db.get_parameter_value_items(parameter_definition_name = parameter_name):
            for year in years:
                try:
                    add_entity(target_db,"asset__asset__year",(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],year))
                except:
                    pass
                try:
                    add_entity(target_db,"asset__asset__year",(parameter_dict["entity_byname"][2],parameter_dict["entity_byname"][3],year))
                except:
                    pass
                add_entity(target_db,"asset_flow__asset_flow",(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],year,parameter_dict["entity_byname"][2],parameter_dict["entity_byname"][3],year))

            if parameter_dict["type"] == "float":
                for year in years:
                    add_parameter_value(target_db,"asset_flow__asset_flow","ratio",parameter_dict["alternative_name"],(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],year,parameter_dict["entity_byname"][2],parameter_dict["entity_byname"][3],year),parameter_dict["parsed_value"])

            elif parameter_dict["type"] == "map":

                map_table = convert_map_to_table(parameter_dict["parsed_value"])
                index_names = nested_index_names(parameter_dict["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")

                if any(i in data.index for i in starttime):
                    for year in data.index:
                        add_parameter_value(target_db,"asset_flow__asset_flow","ratio",parameter_dict["alternative_name"],(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],year[1:],parameter_dict["entity_byname"][2],parameter_dict["entity_byname"][3],year[1:]),data.at[year,"value"])
                
                if any(i in data.index for i in starttime_sp):
                    for index, element in enumerate(starttime_sp):
                        try:
                            alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                            add_alternative(target_db,alternative_name)
                        except:
                            pass
                        steps = pd.to_timedelta(duration) / pd.to_timedelta(resolution)
                        mean_data = data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")].mean()
                        for year in years:
                            add_parameter_value(target_db,"asset_flow__asset_flow","ratio",f"wy{str(pd.Timestamp(element).year)}",(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],year,parameter_dict["entity_byname"][2],parameter_dict["entity_byname"][3],year),float(mean_data))
            
            if "CO2" in parameter_dict["entity_byname"][1]:
                if not source_db.get_parameter_value_item(entity_class_name = "unit__to_node", parameter_definition_name = "capacity", alternative_name = "Base", entity_byname = (parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1])):
                    for yearc in yearsc:
                        entity_class_co2  = "asset__asset__commission"
                        entity_byname_co2 = (parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],yearc)
                        try:
                            add_entity(target_db,entity_class_co2,entity_byname_co2)
                        except:
                            pass
                        add_parameter_value(target_db,entity_class_co2,"capacity_coefficient","Base",entity_byname_co2,0.0)
    try:
        target_db.commit_session("Added flows")
    except:
        print("commit flows error")

def add_costs(source_db,target_db):

    # commission parameters
    target_parameters = {"investment_cost": "investment_cost","storage_investment_cost":"investment_cost_storage_energy", "fixed_cost": "fixed_cost","storage_fixed_cost":"fixed_cost_storage_energy"}
    target_commission = {"node__to_unit":"asset__commission","unit__to_node":"asset__commission","node":"asset__commission","link":"asset__asset__commission"}    
    yearsc = [year["name"] for year in target_db.get_entity_items(entity_class_name = "commission")]
    for source_parameter in target_parameters:

        parameter_list = source_db.get_parameter_value_items(parameter_definition_name = source_parameter)
        if parameter_list:
            for parameter_dict in parameter_list:
                
                if parameter_dict["type"] == "map":
                    periods = [i[1:] for i in parameter_dict["parsed_value"].indexes]
                    dict_values = {period:parameter_dict["parsed_value"].values[periods.index(period)] for period in periods}
                elif parameter_dict["type"] == "float":
                    periods = [min(yearsc)]
                    dict_values = {period:parameter_dict["parsed_value"] for period in periods}
                
                for index_ in periods:
                    if parameter_dict["entity_class_name"] in ["node__to_unit","unit__to_node"]:
                        target_entity_class   = target_commission[parameter_dict["entity_class_name"]]
                        target_unit = parameter_dict["entity_byname"][1] if parameter_dict["entity_class_name"] == "node__to_unit" else parameter_dict["entity_byname"][0]
                        target_entity_bynames = [(target_unit,index_)]
                    elif parameter_dict["entity_class_name"] == "link":
                        target_entity_class   = target_commission[parameter_dict["entity_class_name"]]
                        target_entity_bynames =  [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],index_) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if parameter_dict["entity_byname"][0] == entity_link["entity_byname"][1]]
                    else:
                        target_entity_class   = target_commission[parameter_dict["entity_class_name"]]
                        target_entity_bynames = [( parameter_dict["entity_byname"][0],index_)]
                    for target_entity_byname in target_entity_bynames:
                        try:
                            add_entity(target_db,target_entity_class,target_entity_byname)
                        except:
                            pass
                        add_parameter_value(target_db,target_entity_class,target_parameters[source_parameter],parameter_dict["alternative_name"],target_entity_byname,dict_values[index_])
                
                
    #milestone parameters
    target_parameters = {"other_operational_cost": "variable_cost","operational_cost":"variable_cost"}
    target_entity_class   = "asset__asset__year"
    yearsm = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    for source_parameter in target_parameters:
        parameter_list = source_db.get_parameter_value_items(parameter_definition_name = source_parameter)
        if parameter_list:
            for parameter_dict in parameter_list:
                if parameter_dict["type"] == "map":
                    periods = [i[1:] for i in parameter_dict["parsed_value"].indexes]
                    dict_values = {period:parameter_dict["parsed_value"].values[periods.index(period)] for period in periods}
                elif parameter_dict["type"] == "float":
                    periods = [min(yearsm)]
                    dict_values = {period:parameter_dict["parsed_value"] for period in periods}
                
                for index_ in periods:
                    if parameter_dict["entity_class_name"] in ["node__to_unit","unit__to_node"]:
                        target_entity_bynames = [(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][1],index_)] if parameter_dict["entity_class_name"] == "unit__to_node" else [(parameter_dict["entity_byname"][1],parameter_dict["entity_byname"][0],index_)]
                    elif parameter_dict["entity_class_name"] == "link":
                        target_entity_bynames =  [(entity_link["entity_byname"][0],entity_link["entity_byname"][2],index_) for entity_link in source_db.get_entity_items(entity_class_name = "node__link__node") if parameter_dict["entity_byname"][0] == entity_link["entity_byname"][1]]
                    elif parameter_dict["entity_class_name"] == "node__link__node":
                        target_entity_bynames =  [(parameter_dict["entity_byname"][0],parameter_dict["entity_byname"][2],index_)]
                    for target_entity_byname in target_entity_bynames:
                        try:
                            add_entity(target_db,target_entity_class,target_entity_byname)
                        except:
                            pass
                        add_parameter_value(target_db,target_entity_class,target_parameters[source_parameter],parameter_dict["alternative_name"],target_entity_byname,dict_values[index_])

    # variable cost from node__to_unit must be turned to unit__to_node    
    try:
        target_db.commit_session("Added costs")
    except:
        print("commit adding costs error")

def add_emissions(source_db,target_db):

    years = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    emission_condition = False
    for param_map in source_db.get_parameter_value_items(entity_class_name="set", parameter_definition_name = "co2_max_cumulative"):
        if param_map:
            emission_condition = True
            add_entity(target_db,"asset",("atmosphere",))
            add_parameter_value(target_db,"asset","type","Base",("atmosphere",),"storage")

            if param_map["type"] == "map":
                # getting periods info
                starttime = {} 
                year_repr = {} 
                for period in json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "period")[0]["value"])["data"]:
                    starttime[period] = json.loads(source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "start_time")["value"])["data"]
                    year_repr[period] = source_db.get_parameter_value_item(entity_class_name = "period", entity_byname = (period,), alternative_name = "Base", parameter_definition_name = "years_represented")["parsed_value"]

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                add_parameter_value(target_db,"asset","capacity_storage_energy","Base",("atmosphere",),data["value"].max())
                for period in data.index:
                    year = period[1:]
                    try:
                        add_entity(target_db,"asset__commission__year",("atmosphere",year,year))
                    except:
                        pass
                    add_parameter_value(target_db,"asset__commission__year","initial_storage_units","Base",("atmosphere",year,year),round(data.at[period,"value"]/data["value"].max()))
            
            elif param_map["type"] == "float":
                add_parameter_value(target_db,"asset","capacity_storage_energy","Base",("atmosphere",),param_map["parsed_value"])
                for year in years:
                    try:
                        add_entity(target_db,"asset__commission__year",("atmosphere",year,year))
                    except:
                        pass
                    add_parameter_value(target_db,"asset__commission__year","initial_storage_units","Base",("atmosphere",year,year),1.0)

    if emission_condition:
        # unit flow coming from fossil nodes
        co2_params = source_db.get_parameter_value_items(entity_class_name="node",parameter_definition_name="co2_content",alternative_name="Base")
        co2_value  = {co2_param["entity_name"]:co2_param["parsed_value"] for co2_param in co2_params if co2_param["entity_name"] != "CO2"}
        
        for unit_entity in source_db.get_entity_items(entity_class_name="unit"):
            unit__from_nodes = [from_node for from_node in co2_value if source_db.get_entity_item(entity_class_name = "node__to_unit",entity_byname = (from_node,unit_entity["name"]))]       
            unit_name = unit_entity["name"]
            if len(unit__from_nodes) > 1:
                exit("Entity using more than one fossil fuel")
                #for from_node in unit__from_nodes:

            if len(unit__from_nodes) == 1:
                add_entity(target_db,"asset__asset",(unit_name,"atmosphere"))
                for year in years:
                    add_entity(target_db,"asset__asset__commission",(unit_name,"atmosphere",year))
                    add_parameter_value(target_db,"asset__asset__commission","capacity_coefficient","Base",(unit_name,"atmosphere",year),0.0)
                    add_entity(target_db,"asset__asset__year",(unit_name,"atmosphere",year))
                    add_entity(target_db,"asset_flow__asset_flow",(unit_name,"atmosphere",year,unit__from_nodes[0],unit_name,year))
                    add_parameter_value(target_db,"asset_flow__asset_flow","ratio","Base",(unit_name,"atmosphere",year,unit__from_nodes[0],unit_name,year),co2_value[unit__from_nodes[0]])

        for entity_items in [element for element in source_db.get_entity_items(entity_class_name="unit__to_node") if "CO2" in element["entity_byname"][1]]:
            entity_byname = entity_items["entity_byname"]
            unit_name, node_out = entity_byname
            add_entity(target_db,"asset__asset",("atmosphere",unit_name))
            for year in years:
                add_entity(target_db,"asset__asset__commission",("atmosphere",unit_name,year))
                add_parameter_value(target_db,"asset__asset__commission","capacity_coefficient","Base",("atmosphere",unit_name,year),0.0)
                add_entity(target_db,"asset__asset__year",("atmosphere",unit_name,year))
                add_entity(target_db,"asset_flow__asset_flow",("atmosphere",unit_name,year,unit_name,node_out,year))
                add_parameter_value(target_db,"asset_flow__asset_flow","ratio","Base",("atmosphere",unit_name,year,unit_name,node_out,year),1.0)

    # missing when entity uses more than one fossil fuel
    try:
        target_db.commit_session("Added emissions")
    except:
        print("commit adding emissions error")

def add_profiles(source_db,target_db):

    years  = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]
    yearsc = [year["name"] for year in target_db.get_entity_items(entity_class_name = "year")]

    duration      = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "duration")[0]["value"])["data"]
    starttime_sp  = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "start_time")[0]["value"])["data"]
    resolution    = json.loads(source_db.get_parameter_value_items(entity_class_name = "solve_pattern", parameter_definition_name = "time_resolution")[0]["value"])["data"]

    parameters = {"storage_state_upper_limit":"max_storage_level","storage_state_lower_limit":"min_storage_level","availability":"availability","profile_fix":"availability","profile_limit_upper":"availability"}
    for parameter in parameters:
        for dict_profile in source_db.get_parameter_value_items(parameter_definition_name = parameter):
            investment_method = "investment_method"
            if dict_profile["entity_class_name"] in ["node__to_unit","unit__to_node"]:
                target_name = dict_profile["entity_byname"][0]  if dict_profile["entity_class_name"] == "unit__to_node" else dict_profile["entity_byname"][1]
                entity_class = "unit"
            elif dict_profile["entity_class_name"] == "node" or dict_profile["entity_class_name"] == "unit" :
                target_name = dict_profile["entity_byname"][0]
                entity_class = dict_profile["entity_class_name"]
                investment_method = "storage_investment_method" if dict_profile["entity_class_name"] == "node" else "investment_method"

            profile_name = target_name+"_"+parameters[parameter]
            add_entity(target_db,"profile",(profile_name,))
            investment_method_value = source_db.get_parameter_value_item(entity_class_name = entity_class, parameter_definition_name = investment_method, alternative_name = "Base", entity_byname = (target_name,))
            if investment_method_value:
                range_yearsc = [min(yearsc)] if investment_method_value["parsed_value"] == "not_allowed" else yearsc
                for yearc in range_yearsc:
                    add_entity(target_db,"asset__commission__profile",(target_name,yearc,profile_name))
                    add_parameter_value(target_db,"asset__commission__profile","profile_type","Base",(target_name,yearc,profile_name),parameters[parameter])
                    if dict_profile["type"] == "float" and parameters[parameter] in ["max_storage_level","max_energy","min_storage_level","min_energy"]:
                        add_parameter_value(target_db,"asset__commission__profile","is_timeframe_profile","Base",(target_name,yearc,profile_name),True)
            for year in years:
                add_entity(target_db,"profile__year",(profile_name,year))

            if dict_profile["type"] == "map":
                map_table = convert_map_to_table(dict_profile["parsed_value"])
                index_names = nested_index_names(dict_profile["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")
                if any(i in data.index for i in starttime_sp):
                    for index, element in enumerate(starttime_sp):
                        try:
                            alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                            add_alternative(target_db,alternative_name)
                        except:
                            pass
                        steps = int(pd.to_timedelta(duration) / pd.to_timedelta(resolution))
                        df_data = data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")].tolist()
                        profile_map = {"type":"map","index_type":"float","index_name":"period","data":{1.0:{"type":"map","index_type":"str","index_name":"timestep","data":dict(zip(range(1,steps+1),df_data))}}}
                        for year in years:
                            add_parameter_value(target_db,"profile__year","profile_period_timestep",alternative_name,(profile_name,year),profile_map)
                         
            elif dict_profile["type"] == "float":
                # timeframe profile
                if  parameters[parameter] in ["max_storage_level","max_energy","min_storage_level","min_energy"]:
                    profile_map = {"type":"map","index_type":"float","index_name":"period","data":{1.0:dict_profile["parsed_value"]}}
                else:
                    profile_map = {"type":"map","index_type":"float","index_name":"period","data":{1.0:{"type":"map","index_type":"str","index_name":"timestep","data":dict(zip(range(1,steps+1),dict_profile["parsed_value"]*np.ones(steps)))}}}
                for year in years:
                    add_parameter_value(target_db,"profile__year","profile_period",dict_profile["alternative_name"],(profile_name,year),profile_map)
                       

    # Flow profile treatment, positive -> inflow, negative -> demand
    for dict_inflow in source_db.get_parameter_value_items(parameter_definition_name = "flow_profile"):
        target_name = dict_inflow["entity_byname"][0]
        node_type = source_db.get_parameter_value_item(entity_class_name = "node", entity_byname = dict_inflow["entity_byname"], parameter_definition_name = "node_type", alternative_name = "Base")["parsed_value"]
        if node_type == "storage":
            investment_method_value = source_db.get_parameter_value_item(entity_class_name = "node", parameter_definition_name = "storage_investment_method", alternative_name = "Base", entity_byname = (target_name,))
            range_yearsc = [min(yearsc)] if investment_method_value["parsed_value"] == "not_allowed" else yearsc
        else:
            range_yearsc = yearsc
        profile_name = target_name+"_"+ ("demand" if (np.mean(dict_inflow["parsed_value"]) if dict_inflow["type"] == "float" else np.mean(dict_inflow["parsed_value"].values)) < 0.0 else "inflow")
        add_entity(target_db,"profile",(profile_name,))
        # based on node type then commission years
        for yearc in range_yearsc:
            add_entity(target_db,"asset__commission__profile",(target_name,yearc,profile_name))
            parameter_type = "demand" if (np.mean(dict_inflow["parsed_value"]) if dict_inflow["type"] == "float" else np.mean(dict_inflow["parsed_value"].values)) < 0.0 else "inflow"
            add_parameter_value(target_db,"asset__commission__profile","profile_type","Base",(target_name,yearc,profile_name),parameter_type)
        for year in years:
                add_entity(target_db,"profile__year",(profile_name,year))
        if dict_inflow["type"] == "map":
            map_table = convert_map_to_table(dict_inflow["parsed_value"])
            index_names = nested_index_names(dict_inflow["parsed_value"])
            data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
            data.index = data.index.astype("string")
            if any(i in data.index for i in starttime_sp):
                for index, element in enumerate(starttime_sp):
                    try:
                        alternative_name = f"wy{str(pd.Timestamp(element).year)}"
                        add_alternative(target_db,alternative_name)
                    except:
                        pass
                    steps = int(pd.to_timedelta(duration) / pd.to_timedelta(resolution))
                    df_data = ((-1 if parameter_type == "demand" else 1.0)*data.iloc[data.index.tolist().index(element):data.index.tolist().index(element)+int(steps),data.columns.tolist().index("value")]).tolist()
                    profile_map = {"type":"map","index_type":"float","index_name":"period","data":{1.0:{"type":"map","index_type":"str","index_name":"timestep","data":dict(zip(range(1,steps+1),df_data))}}}
                    for year in years:
                        add_parameter_value(target_db,"profile__year","profile_period_timestep",alternative_name,(profile_name,year),profile_map)
                         
        elif dict_inflow["type"] == "float":
            profile_map = {"type":"map","index_type":"float","index_name":"period","data":{1.0:{"type":"map","index_type":"str","index_name":"timestep","data":dict(zip(range(1,steps+1),dict_inflow["parsed_value"]*np.ones(steps)))}}}
            for year in years:
                add_parameter_value(target_db,"profile__year","profile_period",dict_inflow["alternative_name"],(profile_name,year),profile_map)

        annual_scales = source_db.get_parameter_value_items(entity_class_name = "node", parameter_definition_name = "flow_annual", entity_byname = dict_inflow["entity_byname"])
        parameter_name = "peak_demand" if (np.mean(dict_inflow["parsed_value"]) if dict_inflow["type"] == "float" else np.mean(dict_inflow["parsed_value"].values)) < 0.0 else "storage_inflows"
        if annual_scales:
            for annual_scale in annual_scales:
                for year in years:
                    try:
                        add_entity(target_db,"asset__year",(target_name,year))
                    except:
                        pass
                    if annual_scale["type"] == "map":
                        map_table = convert_map_to_table(annual_scale["parsed_value"])
                        index_names = nested_index_names(annual_scale["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                        data.index = data.index.astype("string")
                        if "y"+year in data.index:
                            add_parameter_value(target_db,"asset__year",parameter_name,annual_scale["alternative_name"],(target_name,year),data.at["y"+year,"value"])
                    elif annual_scale["type"] == "float":
                        add_parameter_value(target_db,"asset__year",parameter_name,annual_scale["alternative_name"],(target_name,year),annual_scale["parsed_value"])
        else:
            for year in years:
                try:
                    add_entity(target_db,"asset__year",(target_name,year))
                except:
                    pass
                add_parameter_value(target_db,"asset__year",parameter_name,"Base",(target_name,year),1.0)
    try:
        target_db.commit_session("Added profiles")
    except:
        print("commit adding profiles error")

if __name__ == "__main__":
    main()

