import json, os
from pyapacheatlas.core.util import GuidTracker, AtlasException
from pyapacheatlas.core import PurviewClient, AtlasEntity, TypeCategory, AtlasProcess
from pyapacheatlas.core.typedef import (EntityTypeDef, AtlasAttributeDef)
import logging, re

from .column_parser import get_column_transformations
from .join_parser import get_join_conditions


class PurviewTransform:
    def __init__(self, client, in_data):
        self.client = client
        self.in_data = in_data
        self._inputs = []
        self._relationship_inputs = []
        self._raw_inputs = []
        self._outputs = []
        self._output_fields = []
        self._raw_outputs = []
        self._relationship_outputs = []
        self._raw_outputs = []
        self._aliases = []
        self._tables = []
        self._alias_tablenames = {}
        self._column_mapping = []
        self._field_mapping = None
        self._col_alias_map = {}
        self._input_cols = []
        self._output_cols = []
        self.hardcodecol = []
        self.input_tables = []
        self.output_table = ""
        self.output_table_schm = ""
        self.joinList = []
        self._table_and_columns = {}
        self.inp_qualified_Name = ""
        self.out_qualified_Name = ""
        self.setproject = 0
        self.table_and_schema_count = {}
        self.sqlcommand = ""
        self._input_table_qualifiedName = {}

        self.inp_name = ""
        self.inp_guid = ""
        self.inp_entitytype = ""

        self.output_name = ""
        self.output_guid = ""
        self.output_entitytype = ""

        self.a = []
        self.tbl = []
        self.ds_create_i = 0

        self.intermediate_tbl_views = []
        self.deltatable = []
        self.globaltempviews = []

        self.nb_name = ""
        self.rowkey = ""
        self.cluster_name = ""
        self.dataframe = "N"

        self.match = 0
        self.unmatch = 0

        logging.info("logger started")
        self.gt = GuidTracker()

        logging.info("finished init")

    def get_tbl_nm(self, inp_tblname, type='out'):
        if inp_tblname.count(",") > 0:
            outtblnm = inp_tblname.split(",")[1].strip(" ").replace('[', '').replace(']', '')
            schnm = inp_tblname.split(",")[0].strip(" ").replace('[', '').replace(']', '')
            
            if type == "inp":
                self.input_tables.append(schnm + "." + outtblnm)
            else:
                self.output_table_schm = schnm + "." + outtblnm

            if outtblnm not in self.deltatable and schnm.lower() != "global_temp":
                # self.deltatable.update({outtblnm: schnm})
                self.deltatable.append(schnm + "." + outtblnm)
            if outtblnm not in self.deltatable and schnm.lower() == "global_temp":
                self.globaltempviews.append(schnm + "." + outtblnm)
            # tblnm = str(child['multipartIdentifier']).replace('[', '').replace(']', '')
        else:
            outtblnm = inp_tblname.replace('[', '').replace(']', '')
            self.intermediate_tbl_views.append(outtblnm)
            if type == "inp":
                self.input_tables.append(outtblnm)
            else:
                self.output_table_schm = outtblnm
            # schnm = ""

        if outtblnm not in self.table_and_schema_count:
            self.table_and_schema_count.update({outtblnm: inp_tblname.count(",")})

        return outtblnm

    def subquery_alias(self, plan):
        if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias':
            if plan['identifier']['name'] != '__auto_generated_subquery_name':
                self._aliases.append(plan['identifier']['name'])

    def unresolved_relation(self, plan):
        if plan['class'] == 'org.apache.spark.sql.catalyst.analysis.UnresolvedRelation':
            tblnm = self.get_tbl_nm(str(plan['multipartIdentifier']), "inp")
            self._tables.append(tblnm)

    def get_fields_pattern(self, sqlcmd, plan):

        if sqlcmd == "INSERT":
            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Project':
                if self.setproject == 0:
                    for project in plan['projectList']:
                        # print("Project Entered")
                        _fields = ""
                        for field in project:
                            if field['class'] == "org.apache.spark.sql.catalyst.expressions.Alias":
                                _fields += field['name'] + "|"
                            if field['class'] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute":
                                _fields += field['nameParts'] + "|"
                            if field['class'] == "org.apache.spark.sql.catalyst.expressions.Literal":
                                if field['value'] is not None:
                                    _fields += "lit(" + field['value'] + ")" + "|"
                                else:
                                    _fields += "lit(null)" + "|"
                            if field['class'] == "org.apache.spark.sql.catalyst.expressions.WindowExpression":
                                _fields += "Row_number()" + "|" + "Partition" + "|" + "Group" + "|"
                        self._output_fields.append(_fields)
                    self.hardcodecol = get_column_transformations(plan['projectList'])
                    self.setproject = 1

            self.subquery_alias(plan)
            self.unresolved_relation(plan)

        if sqlcmd == "CreateViewCommand":
            for child in plan['child']:
                if child['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Project':
                    if self.setproject == 0:
                        for project in child['projectList']:
                            # print(project)
                            _fields = ""
                            for field in project:
                                if field['class'] == "org.apache.spark.sql.catalyst.expressions.Alias":
                                    _fields += field['name'] + "|"
                                if field['class'] == "org.apache.spark.sql.catalyst.expressions.AttributeReference":
                                    _fields += field['name'] + "|"
                            self._output_fields.append(_fields)
                        self.hardcodecol = get_column_transformations(child['projectList'])
                        self.setproject = 1
                self.subquery_alias(child)
                self.unresolved_relation(child)
                if not self._tables:
                    self._tables.append(self.nb_name + "_dataframe")
                    self.dataframe = "Y"

        if sqlcmd == "CREATEVIEW":
            for child in plan['child']:
                if child['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Project':
                    if self.setproject == 0:
                        for project in child['projectList']:
                            _fields = ""
                            for field in project:
                                if field['class'] == "org.apache.spark.sql.catalyst.expressions.Alias":
                                    _fields += field['name'] + "|"
                                if field['class'] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute":
                                    _fields += field['nameParts'] + "|"
                                if field['class'] == "org.apache.spark.sql.catalyst.analysis.UnresolvedFunction":
                                    if field['name']['funcName'] == "DENSE_RANK":
                                        _fields += "DENSE_RANK||||" + "|"
                                if field['class'] == "org.apache.spark.sql.catalyst.expressions.Literal":
                                    if field['value'] is not None:
                                        _fields += "lit(" + field['value'] + ")" + "|"
                                    else:
                                        _fields += "lit(null)" + "|"
                                if field['class'] == 'org.apache.spark.sql.catalyst.analysis.UnresolvedStar':
                                    print("*")
                                if field['class'] == "org.apache.spark.sql.catalyst.expressions.WindowExpression":
                                    _fields += "Row_number()" + "|" + "Partition" + "|" + "Group" + "|"
                            self._output_fields.append(_fields)
                        self.hardcodecol = get_column_transformations(child['projectList'])
                        self.setproject = 1

                self.subquery_alias(child)
                self.unresolved_relation(child)

        if sqlcmd == "DS_CREATETABLE":
            _output_proj = []
            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias':
                self.ds_create_i += 1
                self.a.append(self.ds_create_i)
                # print(plan['identifier']['name'])
                self.tbl.append(plan['identifier']['name'])

            if plan['class'] == 'org.apache.spark.sql.execution.datasources.LogicalRelation':
                self.ds_create_i += 1

            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Project':
                self.ds_create_i += 1
                if self.setproject == 0:
                    for project in plan['projectList']:
                        _fields = ""
                        for field in project:
                            if field['class'] == "org.apache.spark.sql.catalyst.expressions.Alias":
                                _fields += field['name'] + "|"
                            if field['class'] == "org.apache.spark.sql.catalyst.expressions.AttributeReference":
                                if field['qualifier']:
                                    qualifier = field['qualifier'].replace('[', '').replace(']', '')
                                    _fields += "[" + qualifier + "," + field['name'] + "]" + "|"
                                else:
                                    _fields += field['name'] + "|"
                        self._output_fields.append(_fields)
                    self.hardcodecol = get_column_transformations(child['projectList'])
                    self.setproject = 1

        if sqlcmd == "Project":
            if self.setproject == 0:
                for project in plan['projectList']:
                    _fields = ""
                    for field in project:
                        if field['class'] == "org.apache.spark.sql.catalyst.expressions.Alias":
                            _fields += field['name'] + "|"
                        if field['class'] == "org.apache.spark.sql.catalyst.expressions.AttributeReference":
                            _fields += field['name'] + "|"
                    self._output_fields.append(_fields)
                self.setproject = 1
            self._tables.append(self.nb_name + "_dataframe")

        if sqlcmd == "MERGE":

            for key, values in plan.items():
                if key == "matchedActions":
                    if values:
                        if 'InsertAction' in values[0][0]['class']:
                            for field in values[0]:
                                if field['class'] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute":
                                    self._output_fields.append(field['nameParts'])
                        else:
                            self.match = 1
                    else:
                        self.match = 1
                if key == "notMatchedActions":
                    if values:
                        if 'InsertAction' in values[0][0]['class']:
                            for field in values[0]:
                                if field['class'] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute":
                                    self._output_fields.append(field['nameParts'])
                        else:
                            self.unmatch = 1
                    else:
                        self.unmatch = 1

            if self.match == 1 and self.unmatch == 1:
                os._exit(0)

            self.subquery_alias(plan)
            self.unresolved_relation(plan)

    def get_parse_plan(self, output_plan):

        for plan in output_plan:
            _alias_tables = ""
            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement':
                for tbl in plan['table']:
                    self.output_table = self.get_tbl_nm(tbl['multipartIdentifier'])
                    self.sqlcommand = "INSERT"

            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.CreateViewStatement':
                # print("hi")
                self.output_table = self.get_tbl_nm(plan['viewName'])
                self.sqlcommand = "CREATEVIEW"

            if plan['class'] == "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement":
                self.output_table = self.get_tbl_nm(plan['tableName'])
                self.sqlcommand = "INSERT"

            if plan['class'] == 'org.apache.spark.sql.execution.datasources.CreateTable':
                self.output_table = self.get_tbl_nm(plan['tableDesc']['identifier']['table'])
                self.sqlcommand = "DS_CREATETABLE"

            if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable':
                self.sqlcommand = "MERGE"

            if plan['class'] == 'org.apache.spark.sql.execution.command.CreateViewCommand':
                self.output_table = self.get_tbl_nm(plan['name']['table'])
                # print(plan['name']['table'])
                self.sqlcommand = "CreateViewCommand"

            self.get_fields_pattern(self.sqlcommand, plan)

        return True

    def _column_alias_map(self, val_list, alias=""):
        if alias == 'NoAlias':
            if 'NoAlias' in self._col_alias_map.keys():
                self._col_alias_map['NoAlias'].append(val_list[0].strip())
            else:
                # create a new array in this slot
                self._col_alias_map['NoAlias'] = [val_list[0].strip()]
        else:
            if val_list[0] in self._col_alias_map:
                self._col_alias_map[val_list[0]].append(val_list[1].strip())
            else:
                # create a new array in this slot
                self._col_alias_map[val_list[0]] = [val_list[1].strip()]

    @staticmethod
    def _column_clean(val):
        return val.replace('[', '').replace(']', '').split(",")

    def get_inp_out_fields(self, _output_fields, sqlcommand):
        print("Raw Source Fields ")
        print(_output_fields)
        if sqlcommand == "MERGE":
            # abc = {}
            # for i in range(len(_output_fields)):
            #     if _output_fields.count(_output_fields[i]) > 1:
            #         if _output_fields[i] not in abc:
            #             abc.update({_output_fields[i]: i})
            # print(abc)
            # for key, value in abc.items():
            #     _output_fields.pop(value)

            odd_i = []
            even_i = []
            for i in range(0, len(_output_fields)):
                if i % 2:
                    even_i.append(_output_fields[i])
                else:
                    odd_i.append(_output_fields[i])

            for i in odd_i:
                val_list = self._column_clean(i)
                if len(val_list) > 1:
                    self._output_cols.append(val_list[1].strip())
                else:
                    self._output_cols.append(val_list[0].strip())
                # self._output_cols.append(i.replace('[', '').replace(']', ''))
            for i in even_i:
                val_list = self._column_clean(i)
                if len(val_list) > 1:
                    self._input_cols.append(val_list[1].strip())
                    self._column_alias_map(val_list)
                else:
                    self._input_cols.append(val_list[0].strip())
                    self._column_alias_map(val_list, "NoAlias")

            self.output_table = self._tables[0]
            self._tables.pop(0)
            self.output_table_schm = self.input_tables[0]
            self.input_tables.pop(0)
        else:
            for val in _output_fields:
                if val.count("|") == 1:
                    val_list = self._column_clean(val.replace('|', ''))
                    if len(val_list) > 1:
                        self._input_cols.append(val_list[1].strip())
                        self._output_cols.append(val_list[1].strip())
                        self._column_alias_map(val_list)
                    else:
                        # self._col_alias_map.update({'NoAlias':val_list[0].strip()})
                        self._input_cols.append(val_list[0])
                        self._output_cols.append(val_list[0])
                        self._column_alias_map(val_list, 'NoAlias')

                if val.count("|") == 2:
                    val_split = val.split("|")[1]
                    if val_split.__contains__("lit"):
                        self._input_cols.append(val_split)
                        # self.hardcodecol.append(val_split + " as " + val.split("|")[0])
                    else:
                        # print(val_split)
                        val_list = self._column_clean(val_split)
                        # print(val_list)
                        if len(val_list) > 1:
                            self._input_cols.append(val_list[1].strip())
                            self._column_alias_map(val_list)
                        else:
                            self._input_cols.append(val_list[0].strip())
                            self._column_alias_map(val_list, 'NoAlias')

                    self._output_cols.append(val.split("|")[0])

                if val.count("|") == 3:
                    val_split = val.split("|")[1]
                    val_list = self._column_clean(val_split)
                    if len(val_list) > 1:
                        self._input_cols.append(val_list[1].strip())
                    else:
                        self._input_cols.append(val_list[0].strip())
                    self._output_cols.append(val.split("|")[0].strip())
                    # self._column_alias_map(val_list)

                # if val.count("|") > 3:
                #     self.hardcodecol.append("Derived Logic like CASE/CONCAT/DENSE_RANK  " + val.split("|")[0])

        print("Input and Output and HardCode and Column_Alias Mapping")
        print(self._input_cols)
        print(self._output_cols)
        print(self.hardcodecol)
        print(self._col_alias_map)

        return True

    # def get_join_conditions(self, output_plan):
    #
    #     for plan in output_plan:
    #         if plan['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Join':
    #             join = (plan['joinType']['object'])[:-1]
    #             joinname = join.split('.')[-1]
    #             tablealias1 = ((plan['condition'][1])['nameParts']).split(',')[0][1:]
    #             column1 = ((plan['condition'][1])['nameParts']).split(',')[1][1:-1]
    #             tablealias2 = ((plan['condition'][2])['nameParts']).split(',')[0][1:]
    #             column2 = ((plan['condition'][2])['nameParts']).split(',')[1][1:-1]
    #             row = self._alias_tablenames[tablealias1] + " " + joinname + "Join" + " " + self._alias_tablenames[
    #                 tablealias2] + " on " + column1 + "=" + column2
    #             self.joinList.append(row)
    #     return True

    def get_alias_table_cols(self):

        for key, value in self._col_alias_map.items():
            if key in self._alias_tablenames:
                if type(value) is list:
                    for col in value:
                        colname = col
                        tablename = self._alias_tablenames.get(key)
                        self._table_and_columns.update({colname: tablename})
                else:
                    colname = value[0]
                    tablename = self._alias_tablenames.get(key)
                    self._table_and_columns.update({colname: tablename})
            else:
                if type(value) is list:
                    for col in value:
                        colname = col
                        tablename = list(self._alias_tablenames.values())[0]
                        self._table_and_columns.update({colname: tablename})
                else:
                    colname = value[0]
                    tablename = list(self._alias_tablenames.values())[0]
                    self._table_and_columns.update({colname: tablename})

        print("Alias Tables and Columns Mapping")
        print(self._alias_tablenames)
        print(self._table_and_columns)

        return True

    def search_purview_entity(self, entityname):

        print(entityname)
        search = self.client.discovery.search_entities(entityname)
        qname, name, guid, entitytype = "", "", "", ""
        for entity in search:
            # re.search(r"^cooked*\W?", entity['qualifiedName'])
            # print(entity)
            try:
                if (entity["entityType"] == "azure_datalake_gen2_path" or str(
                        entity["entityType"]).lower() == "dataset") and \
                        str(entity['name']).lower().strip() == entityname.lower() and re.search(
                    r"^" + self.cluster_name + "://", entity['qualifiedName']):
                    qname = entity['qualifiedName']
                    name = entity['name']
                    guid = entity['id']
                    entitytype = entity['entityType']

                    # print(entity['qualifiedName'])
                    # print("ABC    "+qname)
            except:
                print("Search Scan results are Coming Differently")

        return qname, name, guid, entitytype

    def get_ds_create_tables(self):
        for j in range(len(self.a) - 1):
            k = self.a[j]
            if self.a[j + 1] == k + 1:
                self._alias_tablenames.update({self.tbl[j]: self.tbl[j + 1]})

    def purview_plan_push(self, qualifiedName, runid):

        print(self._tables)
        print(self.table_and_schema_count)

        purv_inpqname, purv_inpname, purv_inpguid, purv_inpentitytype = "", "", "", ""
        purv_outqname, purv_outname, purv_outguid, purv_outentitytype = "", "", "", ""

        for inp in self._tables:
            # Need to convert it into a Function
            if inp in self.table_and_schema_count and self.table_and_schema_count[inp] > 0:
                print(inp + "  hiii " + str(self.table_and_schema_count[inp]))
                # purv_inpqname, purv_inpname, purv_inpguid, purv_inpentitytype = self.search_purview_entity(inp.lower())

            # print(purv_inpqname)
            if purv_inpqname:
                # print("Here" + "    " + purv_qname)
                self.inp_qualified_Name = purv_inpqname
                self.inp_name = purv_inpname
                self.inp_guid = purv_inpguid
                self.inp_entitytype = purv_inpentitytype
            else:
                self.inp_qualified_Name = (qualifiedName + "://" + inp).lower()
                self.inp_name = inp
                self.inp_guid = self.gt.get_guid()
                self.inp_entitytype = "DataSet"

            self._input_table_qualifiedName.update({inp: self.inp_qualified_Name})
            self._inputs.append(AtlasEntity(name=self.inp_name,
                                            typeName=self.inp_entitytype,
                                            qualified_name=self.inp_qualified_Name,
                                            guid=self.inp_guid)
                                )

        print("Input Tables and Its Qualified Names")
        print(self._input_table_qualifiedName)

        if self.sqlcommand == "INSERT" or self.sqlcommand == "MERGE":
            print("INSERT Occured Hence Output Table will be Searched")
            # purv_outqname, purv_outname, purv_outguid, purv_outentitytype = self.search_purview_entity(self.output_table)

        if purv_outqname:
            self.out_qualified_Name = purv_outqname
            self.output_name = purv_outname
            self.output_guid = purv_outguid
            self.output_entitytype = purv_outentitytype
        else:
            self.out_qualified_Name = (qualifiedName + "://" + self.output_table).lower()
            self.output_name = self.output_table
            self.output_guid = self.gt.get_guid()
            self.output_entitytype = "DataSet"

        print(self.output_table + "   " + self.out_qualified_Name)

        OutputTable = AtlasEntity(
            name=self.output_name,
            typeName=self.output_entitytype,
            qualified_name=self.out_qualified_Name,
            guid=self.output_guid
        )
        if self._input_cols and self._output_cols:
            for item in zip(self._input_cols, self._output_cols):
                if item[0] in self._table_and_columns:
                    inp = self._input_table_qualifiedName.get(self._table_and_columns.get(item[0]))
                    self._column_mapping.append(
                        {"ColumnMapping": [
                            {"Source": item[0], "Sink": item[1]}],
                            "DatasetMapping": {"Source": inp,
                                               "Sink": OutputTable.qualifiedName}})

        print("ColumnMapping")
        print(self._column_mapping)
        # print(self.hardcodecol)
        # print(self.joinList)
        process = AtlasProcess(
            name=qualifiedName + "_" + self.output_table + "_process",
            typeName="HRServicesInsights_OneHRSI",
            qualified_name="hrdi://synapse_notebook/" + self.nb_name + "/" + qualifiedName + "_" + self.output_table,
            inputs=self._inputs,
            outputs=[OutputTable],
            guid=self.gt.get_guid(),
            attributes={"columnMapping": json.dumps(self._column_mapping),
                        "hardCoded_Columns": self.hardcodecol,
                        "Delta_Tables": self.deltatable,
                        "Global_Temp_Views_or_Tables": self.globaltempviews,
                        "Intermediate_Views_or_Tables": self.intermediate_tbl_views,
                        "JoinConditions": self.joinList}
        )
        if self._inputs and OutputTable:
            try:
                results = self.client.upload_entities([process, OutputTable] + self._inputs)
            except:
                print("No ColumnMapping or Input or Output Tables Available")

    def purview_dataset_push(self, inp_qname, inp_name, out_qname, out_name, process_qname, name):
        print("Came")
        a = AtlasEntity(
            name=inp_name,
            typeName="DataSet",
            qualified_name=inp_qname,
            guid=self.gt.get_guid()
        )
        b = AtlasEntity(
            name=out_name,
            typeName="DataSet",
            qualified_name=out_qname,
            guid=self.gt.get_guid()
        )

        process = AtlasProcess(
            name=name,
            typeName="Process",
            qualified_name="Process" + process_qname,
            inputs=[a],
            outputs=[b],
            guid=self.gt.get_guid()
        )

        results = self.client.upload_entities(batch=[a, b, process])

    def get_project_details(self, output_plan):
        for plan in output_plan:
            _alias_tables = ""
            if plan['class'] == "org.apache.spark.sql.catalyst.plans.logical.Project":
                if plan['projectList'][0][0]['qualifier']:
                    # print(plan['projectList'][0][0]['qualifier'])
                    self.output_table = self.get_tbl_nm(plan['projectList'][0][0]['qualifier'])
                    self.sqlcommand = "Project"
                else:
                    os._exit(0)

            self.get_fields_pattern(self.sqlcommand, plan)
        return True

    def transform_to_purview(self):

        inputs_array = self.in_data['inputs']
        outputs_array = self.in_data['outputs']
        runid = self.in_data['run']['runId']

        _name = self.in_data['job']['name'].split(".")[0].split("_")[:-2]
        self.rowkey = "_".join(_name) + "_" + runid
        self.nb_name = "_".join(_name) 

        if re.search("hrsi", self.in_data['job']['name'].lower()):
            self.cluster_name = "hrservicesinsights"
        elif re.search("ultp", self.in_data['job']['name'].lower()):
            self.cluster_name = "ultp_services"
        elif re.search("gtabi", self.in_data['job']['name'].lower()):
            self.cluster_name = "gtabi_services"
        elif re.search("learning", self.in_data['job']['name'].lower()):
            self.cluster_name = "learninginsights"
        elif re.search("hcm", self.in_data['job']['name'].lower()):
            self.cluster_name = "headcountmanagement"
        else:
            self.cluster_name = "external"

        # qualifiedName = self.cluster_name + "://" + self.nb_name
        qualifiedName = self.cluster_name
        print(qualifiedName)
        # print(len(self.in_data['run']['facets']))

        if len(self.in_data['run']['facets']) > 1:

            _plan = self.in_data['run']['facets']['spark.logicalPlan']['plan']
            print(runid + "  " + self.nb_name)

            classname = self.in_data['run']['facets']['spark.logicalPlan']['plan'][0]['class']

            if classname == "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand":
                for inp in self.in_data['inputs']:
                    table = inp['name'].split("/")[-1]
                    self.input_tables.append(table)
                    for col in inp['facets']['schema']['fields']:
                        column = col['name']
                        self._input_cols.append(column)
                        self._table_and_columns.update({column: table})
                self.output_table_schm = self.in_data['outputs'][0]['name'].split("/")[-1]
                self.input_tables = list(dict.fromkeys(self.input_tables))
                self._input_cols = list(dict.fromkeys(self._input_cols))
                self.output_table = self.output_table_schm
                self._output_cols = self._input_cols
                self.deltatable = self.output_table
                self._tables = self.input_tables

                print(self.input_tables)
                print(self.output_table_schm)
                print(self._input_cols)
                print(self._output_cols)
                print(self._table_and_columns)

            else:
                if classname == "org.apache.spark.sql.catalyst.plans.logical.Project":
                    self.get_project_details(_plan)
                else:
                    self.get_parse_plan(_plan)

                if self.sqlcommand == "DS_CREATETABLE":
                    self.get_inp_out_fields(self._output_fields, self.sqlcommand)
                    self.get_ds_create_tables()
                else:
                    self.get_inp_out_fields(self._output_fields, self.sqlcommand)

                if self._aliases and self._tables:
                    if len(self._aliases) == len(self._tables):
                        for item in zip(self._aliases, self._tables):
                            self._alias_tablenames.update({item[0]: item[1]})
                    else:
                        _aliases_new = self._aliases[1:]
                        for item in zip(_aliases_new, self._tables):
                            self._alias_tablenames.update({item[0]: item[1]})
                elif self._tables:
                    self._alias_tablenames.update({'NoAlias': self._tables[0]})

                if self._col_alias_map and self._alias_tablenames:
                    self.get_alias_table_cols()

                if self.sqlcommand in ("CreateViewCommand", "CREATEVIEW"):
                    self.joinList = get_join_conditions(_plan[0]['child'], self._alias_tablenames)
                elif self.sqlcommand == "INSERT":
                    self.joinList = get_join_conditions(_plan, self._alias_tablenames)

                if self.dataframe == "Y":
                    inp_qname = self.out_qualified_Name
                    inp_name = self.output_name
                    out_name = self.nb_name + "_dataframe"
                    out_qname = (self.cluster_name + "://" + self.nb_name + "://" + out_name).lower()
                    # out_qname = (self.cluster_name + "://" + out_name).lower()

                    # print(inp_name)
                    self.purview_dataset_push(inp_qname, inp_name, out_qname, out_name, inp_name, "TabletoDataframe")

                print("Output Table")
                print(self.output_table)
                print(self.output_table_schm)
                print(self.input_tables)
                print(self.deltatable)
                print(self.globaltempviews)
                print(self.intermediate_tbl_views)

                print(self._tables)
            self.purview_plan_push(qualifiedName, runid)

        else:
            print("Facets doesnt have any Plan")
            inp_qname, inp_name = "", ""
            if inputs_array:
                for inp in inputs_array:
                    inp_qname = dict(inp).get('name')
                    inp_name = str(dict(inp).get('name')).split("/")[-1].split('.')[0]
                out_name = self.nb_name + "_dataframe"
                out_qname = self.cluster_name + "://" + self.nb_name + "://" + out_name
                print(inp_name)
                self.purview_dataset_push(inp_qname.lower(), inp_name, out_qname.lower(), out_name, out_name,
                                          "FiletoDataframe")
            else:
                os._exit(0)

        return self.cluster_name, self.rowkey, self.input_tables, self.output_table_schm, self._input_cols, self._output_cols, self.deltatable, self.intermediate_tbl_views, self.globaltempviews, self.hardcodecol, self.joinList
