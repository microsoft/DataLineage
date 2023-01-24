# import json
# from collections.abc import Mapping


def unresolvedAttribute(string):
    a = string.strip('[]').split(',')
    return (".".join(i.strip() for i in a))


def literal(rec):
    elem = ""
    if rec["dataType"] == "string":
        elem = "'" + rec["value"] + "'"
    elif rec["dataType"] == "null":
        elem = "NULL"
    else:
        elem = rec["value"]
    return elem


def attribute(rec):
    str = ""
    if rec["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute":
        str = unresolvedAttribute(rec["nameParts"])
    elif rec["class"] == "org.apache.spark.sql.catalyst.analysis.Literal" or rec[
        "class"] == "org.apache.spark.sql.catalyst.expressions.Literal":
        str = literal(rec)
    elif rec["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedStar":
        str = "*"
    return str


def WindowSpecDefinition(row, index):
    str = ""
    # print(index)
    curr_index = index + 1
    if len(row[index]["partitionSpec"]) > 0:
        children = len(row[index]["partitionSpec"])
        no_of_children = 0
        str += "PARTITION BY "
        while no_of_children < children and curr_index < len(row):
            temp = ""
            if row[curr_index]["num-children"] > 0:
                temp, curr_index = function(row, curr_index)
            else:
                temp = attribute(row[curr_index])
            str += temp
            if no_of_children < children - 1:
                str += ", "
            curr_index += 1
            no_of_children += 1
    if len(row[index]["orderSpec"]) > 0:
        str += " ORDER BY "
        if row[curr_index]["num-children"] == 1:
            temp = ""
            if row[curr_index]["direction"]["object"] == "org.apache.spark.sql.catalyst.expressions.Descending$":
                temp = " DESC"
            else:
                temp = " ASC"
            curr_index += 1
            children = len(row[index]["orderSpec"])
            no_of_children = 0
            while no_of_children < children and curr_index < len(row):
                temp2 = ""
                if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.SortOrder":
                    if row[curr_index]["direction"]["object"] == "org.apache.spark.sql.catalyst.expressions.Descending$":
                        temp = " DESC"
                    else:
                        temp = " ASC"
                    curr_index += 1
                    continue
                elif row[curr_index]["num-children"] > 0:
                    temp2, curr_index = function(row, curr_index)
                else:
                    temp2 = attribute(row[curr_index])
                str += temp2
                str += temp
                if no_of_children < children - 1:
                    str += ", "
                curr_index += 1
                no_of_children += 1
                # print(curr_index)

    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame$":
        curr_index += 1
        pass
    return (str, curr_index - 1)


def windowExpression(row, index):
    fstr = ""
    curr_index = index + 1
    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedFunction":
        str, curr_index = function(row, curr_index)
        fstr += str
        curr_index += 1
    fstr += " OVER ("
    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition":
        str, curr_index = WindowSpecDefinition(row, curr_index)
        fstr += str
        curr_index += 1
    fstr += ")"
    return (fstr, curr_index - 1)


def recurse(row, index):
    children = row[index]["num-children"]
    no_of_children = 0
    curr_index = index + 1
    while no_of_children < children and curr_index < len(row):
        curr_index = recurse(row, curr_index)
        curr_index += 1
        no_of_children += 1
    return curr_index - 1


def function(row, curr_index):
    funcName,funcStr, index = "", "", ""
    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.CaseWhen":
        return ("CASE WHEN function", recurse(row, curr_index))
    elif row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.Cast":
        funcName = "CAST"
    # elif row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.Add":
    #     pass
    elif row[curr_index]["class"] == "org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame":
        return (funcStr, index)
    elif row[curr_index]["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias":
        pass
    else:
        if "name" in row[curr_index]:
            funcName = row[curr_index]["name"]["funcName"]
        else:
            funcName = row[curr_index]["class"].split(".")[-1]
    children = row[curr_index]["num-children"]
    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias":
        funcStr = ""
    else:
        funcStr = f"{funcName}("
    index = curr_index + 1
    no_of_children = 0
    while no_of_children < children and index < len(row):
        elem = row[index]
        str = ""
        if elem["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedFunction":
            str, index = function(row, index)
        elif elem["class"] == "org.apache.spark.sql.catalyst.expressions.Cast":
            str, index = function(row, index)
        elif elem["class"] == "org.apache.spark.sql.catalyst.expressions.WindowExpression":
            str, index = windowExpression(row, index)
        elif elem["num-children"] == 0:
            str = attribute(elem)
        else:
            str, index = function(row, index)
        funcStr += str
        if no_of_children < children - 1:
            funcStr += ", "
        index += 1
        no_of_children += 1
    if funcName == "CAST":
        dataType = row[curr_index]["dataType"].upper()
        funcStr += f" AS {dataType}"

    if row[curr_index]["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias":
        pass
    else:
        funcStr += ")"
    return (funcStr, index - 1)


def get_column_transformations(parseJson):
    output = []
    for line in parseJson:
        # print(len(line)>1)
        firstElem = line[0]
        alias = ""
        index = 0
        if firstElem["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias":
            pass
        if firstElem["class"] == "org.apache.spark.sql.catalyst.expressions.Alias":
            alias = firstElem["name"]
            index += 1
        elif firstElem["num-children"] == 0:
            alias = attribute(firstElem)
            index += 1
        # print("sdc")
        if len(line) > 1:
            # print("Dsc")
            funcStr = ""
            while index < len(line):
                if line[index]["class"] == "org.apache.spark.sql.catalyst.expressions.WindowExpression":
                    funcStr, index = windowExpression(line, index)
                # elif line[index]["num-children"] == 0:
                #     funcStr = attribute(line[index])
                elif line[index]["num-children"] > 0:
                    funcStr, index = function(line, index)
                index += 1
            if funcStr != "":

                # print(index)
                if firstElem["class"] == "org.apache.spark.sql.catalyst.analysis.UnresolvedAlias":
                    output += [funcStr]
                    # print(funcStr)
                else:
                    output += [funcStr + " AS " + alias]
                    # print(funcStr + " AS " + alias)
        # else:
        #     print(alias)
        # elif
    if len(output) == 0:
        return None
    else:
        return output


# def findProjectList(elem, count):
#     output = []
#     if isinstance(elem, Mapping) or isinstance(elem, list):
#         if "projectList" in elem:
#             temp = parser(elem["projectList"])
#             # print("")
#             # if temp is not None:
#             output += [temp]
#         else:
#             for item in elem:
#                 if isinstance(elem, Mapping):
#                     temp = findProjectList(elem[item], count + 1)
#                 else:
#                     temp = findProjectList(item, count + 1)
#                 if temp is not None:
#                     output += temp
    # if len(output) > 0:
    # print("Output List",output)
    # print("-------------------------------")
    # print("-------------------------------")
    # if len(output) > 0:
    #     return output
    # else:
    #     return None


# cnt=0
# data = json.load(open('dimperson/dimperson_create6.json'))
# # data= json.load(open('Yogesh (1)/dimcrm1.json'), object_pairs_hook=OrderedDict)
#
# tablePlan = data["run"]["facets"]["spark.logicalPlan"]["plan"]
# for i in tablePlan:
#     # cnt+=1
#     # print(cnt)
#     result = findProjectList(i, 0)
#     if result is not None:
#         print()
#         print("Result=", result)
#         print("-------------------------------")
        # break
    # if len(result) > 0:
    # print(result)
    # print(result)
