import datetime, time
import argparse
import keyring
import pdb
from hdbcli import dbapi
import glob, os
import itertools
import xml.etree.ElementTree as ET
import xlsxwriter
from collections import defaultdict


def parse_args():
    '''
    Fetch command line arguments
    '''
    l_desc = 'This program will generate calculation view documentation. Required parameters: --host: hana server, '+\
             '--calcviewdir: directory where calculation view files are. --output_file: name of output directory. \r\nVersion: 1.0'
    parser = argparse.ArgumentParser(description = l_desc)
    parser.add_argument("--host", "-ho", help="<Required> Hostname of the HANA server", required=True)
    parser.add_argument("--port", "-po", default = '30015', help="Port of the HANA server, default: 30015")
    parser.add_argument("--user", "-us", default = 'SYSTEM', help="User for the HANA server - needs appriopriate read-rights on the HDB module schema, default: SYSTEM")
    parser.add_argument("--schema", "-sc", default = 'HDB_DATAMART', help="Schema where the calculation views are stored, default: HDB_DATAMART")
    parser.add_argument("--calcviewdir", "-cv", help="<Required> Calculation view directory. Export calculation view folder, unzip to a directory. Add that directory to this parameter.", required = True)
    parser.add_argument("--output_file", "-of", help="<Required> Output file. Make sure to include the complete path and file with extension: xlsx.", required = True)
    args = parser.parse_args()
    o_params = {}
    o_params['host'] = args.host
    o_params['port'] = args.port
    o_params['user'] = args.user
    o_params['schema'] = args.schema
    o_params['password'] = keyring.get_password(o_params['host'],'password')
    assert keyring.get_password(o_params['host'],'password') != None, 'Password for db user should be added to keyring in python. E.g. keyring.set_password("'+str(o_params['host'])+'", "password", "init1234")'
    o_params['cv_dir'] = args.calcviewdir
    o_params['output_file'] = args.output_file
    return o_params


def hana_connect_cli(i_host, i_port, i_user, i_pw):
    '''
    Connect to the hana server via hdbcli
    Input:
    - i_host: hostname / ip
    - i_port: port
    - i_user: user
    - i_pw: password
    Output: 
    - o_cursor: cursor connected to HANA
    - o_connection: connection - must be closed afterwards
    '''
    o_connection = dbapi.connect(
        address=i_host,
        port=i_port,
        user=i_user,
        password=i_pw
    )
    o_cursor = o_connection.cursor()
    return o_cursor, o_connection


def fetch_views(i_cursor, i_schema):
    '''
    Fetch views and columns from system views VIEWS and VIEW_COLUMNS
    '''
    _qry = "SELECT VIEW_NAME FROM VIEWS where SCHEMA_NAME = '"&i_schema&"' and VIEW_TYPE = 'CALC';"
    i_cursor.execute(_qry)
    _tables = ','.join(["'"+x[0]+"'" for x in i_cursor.fetchall()])
    _qry = "SELECT VIEW_NAME, COLUMN_NAME, POSITION, DATA_TYPE_NAME, LENGTH, SCALE FROM VIEW_COLUMNS WHERE VIEW_NAME in ("+\
        _tables+\
        ") and SCHEMA_NAME = '"&i_schema&"';"
    i_cursor.execute(_qry)
    _columns = i_cursor.fetchall()
    o_result = []
    for _table in _tables.replace("'","").split(","):
        o_result.append([_table,[x for x in _columns if x[0]==_table]])
    return o_result


class CalcViewXmlObject():
    '''Calculation View XML object, contains all calculation view documentation'''
    def __init__(self,xml_dict):
        '''init CalcViewXmlObject, set variable: xml_dict'''
        self.xml_dict = xml_dict
    def set_datasources(self):
        self.datasources = [child.attrib['id'] for child in self.xml_dict.findall('dataSources/DataSource')]
        self.calc_views = [child.attrib['id'] for child in self.xml_dict.findall('calculationViews/calculationView')]
        self.calc_view_inputs = {}
        for calc_view in self.calc_views+self.datasources:
            self.calc_view_inputs[calc_view] = [[child.attrib['id'],child.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']] for child in self.xml_dict.findall("./calculationViews/calculationView/input[@node='"+calc_view+"']/..")]
    def set_node_flow(self):
        '''for each datasource, generate a node_flow variable with node_flow_field object variables'''
        self.node_flow = {}
        self.node_flow_fields = {}
        self.node_flow_calc = {}
        for _datasource in self.datasources:
            self.gen_flow_upwards([_datasource], {})
        # loop through calculation flow
        for _calc_node in self.node_flow_calc:
            _node_chain_fields = {key: value for (key, value) in self.node_flow_calc.items() if key == _calc_node }
            self.gen_flow_upwards([item for item in _calc_node], _node_chain_fields)
    def gen_flow_upwards(self,_node_chain, _node_chain_fields):
        '''generate flow information, going upwards from the datasource. Recursive function.'''
        while True:
            #Find the node that is connected to the last node in the node_chain
            _node_info = [y for x,y in self.calc_view_inputs.items() if x == _node_chain[-1]][0]
            _node = [x[0] for x in _node_info]
            _last_ncf_key = [x for x in _node_chain_fields if x[1] == _node_chain[-1]]
            _last_target_fields = [x['target'] for x in _node_chain_fields[_last_ncf_key[0]]] if _last_ncf_key != [] else []
            if _last_target_fields == [] and _last_ncf_key != []:
                #no fields used in following target, stop loop
                break
            if len(_node) == 0:
                #Get logical node - can be projection or aggregation, but with field renames and mappings
                _node_chain += ['logicalModel']
                _node_chain_fields[(_node_chain[-2],_node_chain[-1])] = self.get_logical_node_info(_last_target_fields, _node_chain)
                #No more connecting nodes, save node_flow_fields and node_flow as object variables, exit the loop
                self.node_flow_fields[_node_chain[0]] = _node_chain_fields
                self.node_flow[_node_chain[0]] = _node_chain
                break
            elif len(_node) == 1:
                #One connecting node, update _node_chain and add _node_chain_fields
                _node_chain += _node
                _node_chain_fields[(_node_chain[-2],_node_chain[-1])] = self.gen_node_info(_node_chain, _last_target_fields, _node_info[0][1])
            elif len(_node) > 1:
                #Multiple connecting nodes upwards, create a new flow upwards for each node, exit this flow
                for _node_item in _node_info:
                    _s_node_chain = _node_chain+[_node_item[0]]
                    _s_node_chain_fields = _node_chain_fields
                    _s_node_chain_fields[(_s_node_chain[-2],_s_node_chain[-1])] = self.gen_node_info(_s_node_chain, _last_target_fields, _node_item[1])
                    self.gen_flow_upwards(_s_node_chain,_s_node_chain_fields)
                break
    def get_logical_node_info(self, _source_fields, _node_chain):
        '''fetch the logical model, the latest step in the calculation view'''
        _mappings = []
        _logical_type = [x[1] for x in self.xml_dict.items() if x[0] == 'outputViewType'][0]
        _attribs = [child for child in self.xml_dict.findall("./logicalModel/attributes/")]
        for _field in _attribs:
            #find the source column name and add it as a list with the target field (id), exclude hidden fields
            _mappings += [{'source':child.attrib['columnName'],'target':_field.attrib['id']} for child in _field.findall("./keyMapping") if 'hidden' not in _field.attrib]
        _measures = [child for child in self.xml_dict.findall("./logicalModel/baseMeasures/")]
        for _field in _measures:
            #find the source column name and add it as a list with the target field (id)
            _mappings += [{'source':child.attrib['columnName'],'target':_field.attrib['id']} for child in _field.findall("./measureMapping") if 'hidden' not in _field.attrib]
        #filter fields that come from the source
        _mappings = [x for x in _mappings if x['source'] in _source_fields] if _source_fields != [] else _mappings
        #fetch all calculations where these fields are used.
        #run through formulas to find if the fields form the source are used here.
        _formulas = [(child, child.attrib['id']) for child in self.xml_dict.findall("./logicalModel/calculatedAttributes/keyCalculation/formula/..")]
        _calc_mappings = []
        _frm_mappings = []
        for _formula, id in _formulas:
            _formulas = [child.text for child in _formula.findall("./formula") if '"' in child.text]
            for _sub_form in _formulas:
                for _mapping in _mappings:
                    if _mapping['target'] in _sub_form:
                        _frm_mappings += [{'source':_mapping['source'],'target':id,'calculation':_sub_form}]
            _formula_wo_source = [child.text for child in _formula.findall("./formula") if '"' not in child.text]
            for _u_formula in _formula_wo_source:
                _calc_mappings += [{'source':'','target':id,'calculation':_u_formula}]
        if _frm_mappings:
            _mappings += _frm_mappings
        if _calc_mappings:
            self.node_flow_calc[('calculation without source',_node_chain[-1])] = _calc_mappings
        return _mappings
    def gen_node_info(self, _node_chain, _source_fields, _node_type):
        '''fetch the node_chain_fields from the last node in the node chain'''
        #fetch input sub-node: mappings. Here we can find any mappings/renames. If it does not exist, it's a 1:1 mapping
        _node_chain_2 = _node_chain[-2] if _node_chain[-2].find('$$$$') == -1 else _node_chain[-2].split('$$$$')[0]
        _node_chain_1 = _node_chain[-1] if _node_chain[-1].find('$$$$') == -1 else _node_chain[-1].split('$$$$')[0]
        #join nodes have a special way of assigning join-fields, only by looking at the central-node in the join do we know where the field comes from
        _filter_join_fields = []
        if _node_type == 'Calculation:JoinView':
            try:
                _jointype = [child.attrib['joinType'] for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']")]
                _input_nodes = [child.attrib['node'] for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']/input")]
                if _jointype[0] == 'leftOuter' and _input_nodes[0] != _node_chain_2 or _jointype[0] == 'rightOuter' and _input_nodes[0] == _node_chain_2:
                    #this is not the central node, make sure to filter out the joinfield(s)
                    _filter_join_fields = [child.attrib['name'] for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']/joinAttribute")]
            except:
                print(f'issue with: {_node_chain_1} from {_node_chain_2}')
                _input_nodes = []
        _mappings = [{'source':child.attrib['source'],'target':child.attrib['target']} for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']/input[@node='"+_node_chain_2+"']/mapping") if 'source' in child.attrib and child.attrib['target'] not in _filter_join_fields]
        if _node_type in ('Calculation:AggregationView','Calculation:ProjectionView','Calculation:RankView','Calculation:JoinView'):
            #if prev_node is "one-input" type, fetch all fields in target node and add to mapping, except renamed nodes that were already added
            _mappings += [{'source':child.attrib['id'],'target':child.attrib['id']} for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']/viewAttributes/viewAttribute") if child.attrib['id'] not in [x['target'] for x in _mappings] and child.attrib['id'] not in _filter_join_fields]
        #filter fields that come from the source
        _mappings = [x for x in _mappings if x['source'] in _source_fields] if _source_fields != [] else _mappings
        #run through formulas to find if the fields form the source are used here.
        _formulas = [(child, child.attrib['id']) for child in self.xml_dict.findall("./calculationViews/calculationView[@id='"+_node_chain_1+"']/calculatedViewAttributes/calculatedViewAttribute/formula/..")]
        _calc_mappings = []
        _frm_mappings = []
        for _formula, id in _formulas:
            _formulas = [child.text for child in _formula.findall("./formula") if '"' in child.text]
            for _sub_form in _formulas:
                for _mapping in _mappings:
                    if _mapping['target'] in _sub_form:
                        _frm_mappings += [{'source':_mapping['source'],'target':id,'calculation':_sub_form}]
            _formula_wo_source = [child.text for child in _formula.findall("./formula") if '"' not in child.text]
            for _u_formula in _formula_wo_source:
                _calc_mappings += [{'source':'','target':id,'calculation':_u_formula}]
        if _frm_mappings:
            _mappings += _frm_mappings
        if _calc_mappings:
            self.node_flow_calc[('calculation without source',_node_chain[-1])] = _calc_mappings
        return _mappings
    def parse_node_flow(self, i_view_fields):
        '''Go through the flow, starting at the logical model node, then travel down to create a target to source mapping per field'''
        _outputmappings = defaultdict(list)
        for _key in self.node_flow:
            #fetch fields in target, and add the source: datasource
            #flow(datasource)=>(source,target)=>(source_field,target_field)
            _nodeflow = self.node_flow_fields[_key]
            _lastkey = [flow for flow in _nodeflow if flow[1]][-1]
            _outputfields = [x['target'] for x in _nodeflow[_lastkey]]
            for _field in _outputfields:
                #Travel through the hierarchy to get the field name of the original table
                _lastkey = [flow for flow in _nodeflow if flow[1]][-1]
                _lastfield = [x['source'] for x in _nodeflow[_lastkey] if x['target'] == _field][0]
                _calculations = []
                while True:
                    try:
                        _lastkey = [flow for flow in _nodeflow if flow[1] == _lastkey[0]][0]
                        _lastpair = [x for x in _nodeflow[_lastkey] if x['target'] == _lastfield]
                        _lastfield = [x['source'] for x in _lastpair]
                        _calculation = [x['calculation'] for x in _lastpair if 'calculation' in x]
                        _calculations += (_calculation) if _calculation else ''
                        _lastfield = _lastfield[0]
                    except IndexError:
                        _lastfield = 'calculation' if _lastfield == '' else _lastfield
                        break
                # empty source field, this shouldn't happen
                pdb.set_trace() if _lastfield == [] else ''
                _field_info = [{'datatype':x[3],'length':x[4],'scale':x[5]} for x in i_view_fields if x[1] == _field]
                _key_str = _key.replace('_SYN','').replace('$$$$','(').replace('$$',')')
                _outputmappings[_key_str+":"+_lastfield+":"+_field] = {'targetfield':_field,'calculation':','.join(_calculations),'field_info':_field_info}
        self.outputmappings = _outputmappings
    def get_node_flow(self):
        return self.node_flow
    def get_node_flow_fields(self):
        return self.node_flow_fields
    def get_outputmappings(self):
        return self.outputmappings


def save_as_file(outputmappings, output_file):
        '''output result to file'''
        workbook = xlsxwriter.Workbook(output_file)
        for name, outputmapping in outputmappings.items():
            name = name if len(name) <= 31 else name[:23] + '...' + name[-5:]
            worksheet = workbook.add_worksheet(name)
            worksheet.write(0,0,'Source table')
            worksheet.write(0,1,'Source column')
            worksheet.write(0,2,'Target column')
            worksheet.write(0,3,'Datatype')
            worksheet.write(0,4,'Length')
            worksheet.write(0,5,'Scale')
            worksheet.write(0,6,'Calculation')
            for i, (k, v) in enumerate(outputmapping.items()):
                worksheet.write(i+1,0,k.split(':')[0])
                worksheet.write(i+1,1,k.split(':')[1])
                worksheet.write(i+1,2,k.split(':')[2])
                worksheet.write(i+1,6,v['calculation'])
                if v['field_info']:
                    worksheet.write(i+1,3,v['field_info'][0]['datatype'])
                    worksheet.write(i+1,4,v['field_info'][0]['length'])
                    worksheet.write(i+1,5,v['field_info'][0]['scale'])
                else:
                    print(f'{name}, field: {k.split(":")[2]}, does not exist as calc view field')
            _tables = list(set([k.split(':')[0] for k,v in outputmapping.items() if '(' not in k.split(':')[0] and k.split(':')[0] != 'calculation without source'] +\
            [k.split(':')[0][k.split(':')[0].find("(")+1:len(k.split(':')[0])-1] for k,v in outputmapping.items() if '(' in k.split(':')[0] and k.split(':')[0] != 'calculation without source']
            ))
            worksheet.write(i+6,0,'Source table')
            worksheet.write(i+6,1,'Description')
            for j, _table in enumerate(_tables):
                worksheet.write(i+j+7,0,_table)
        workbook.close()


def read_cv(i_params,_view_data):
    '''
    Read calculation view data from file.
    '''
    _path = i_params['cv_dir']
    _calc_view_xml_dict = {}
    for filename in glob.glob(os.path.join(_path, '*.hdbcalculationview')):
        with open(filename) as xml_file:
            _calc_view_xml_dict[filename.split('\\')[-1].split('.')[0]] = CalcViewXmlObject(ET.parse(xml_file).getroot())
        xml_file.close()
    _results = defaultdict(list)
    for name, _calc_view_xml in _calc_view_xml_dict.items():
        print(name)
        _calc_view_xml.set_datasources()
        _calc_view_xml.set_node_flow()
        try:
            _view_fields = [x for x in _view_data if x[0] == name][0][1]
        except:
            #calculation view is not present in the database
            pdb.set_trace()
        _calc_view_xml.parse_node_flow(_view_fields)
        _results[name]=(_calc_view_xml.get_outputmappings())
    save_as_file(_results,i_params['output_file'])


def main_flow(i_params):
    '''
    main program flow
    '''
    _cursor, _connection = hana_connect_cli(i_params['host'],i_params['port'],i_params['user'],i_params['password'])
    _view_data = fetch_views(_cursor, i_params['schema'])
    read_cv(i_params,_view_data) if i_params['cv_dir'] != None else print('Missing calculation view directory, param cv_dir. Exiting...')


if __name__ == '__main__': 
    l_start = time.time()
    print('Program start: ' + str(datetime.datetime.now()))
    #Parse the input arguments
    _params = parse_args()
    main_flow(_params)
    l_end = time.time()
    print('End of program. ' + str(datetime.datetime.now()) + '. Runtime in seconds: ' + str(datetime.timedelta(seconds=time.time() - l_start)))