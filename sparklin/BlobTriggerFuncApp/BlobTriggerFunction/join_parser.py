
def get_join_conditions(output_plan,_alias_tablenames):
    # print("get joins info")
    overall_conditions = []
    mid_expressions = []
    attr_expressions = []
    attr_functions = []
    attr_output = []
    intermediate_output = []
    aliases = []
    joinList = []
    count = 0

    for child in output_plan:

        finalout = ""
        if child['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Join':
            join = (child['joinType']['object'])[:-1]
            joinname = join.split('.')[-1]

            if (joinname == "LeftOuter"):
                joinname = "LOJ"
            elif (joinname == "RightOuter"):
                joinname = "ROJ"
            elif (joinname == "Inner"):
                joinname = "IJ"
            elif (joinname == "FullOuter"):
                joinname = "FOJ"

            for i in range(len(child['condition'])):

                childclass = child['condition'][i]['class']

                if childclass != 'org.apache.spark.sql.catalyst.expressions.And' and childclass != 'org.apache.spark.sql.catalyst.expressions.Or':
                    if childclass != 'org.apache.spark.sql.catalyst.expressions.EqualTo':
                        if (
                                childclass != 'org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute' and childclass != 'org.apache.spark.sql.catalyst.expressions.Literal'):

                            if childclass == "org.apache.spark.sql.catalyst.analysis.UnresolvedFunction":
                                attr_functions.append(child['condition'][i]['name']['funcName'])
                            # this is for expressions on attribute like isnotnull OR CAST
                            else:
                                func = childclass.split('.')[-1]
                                if func == 'Cast':
                                    datatype = child['condition'][i]['dataType']
                                    func = "Cast AS " + datatype
                                attr_expressions.append(func)

                        else:
                            if childclass == 'org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute':
                                if (child['condition'][i])['nameParts'].find(",") > 0:
                                    column = ((child['condition'][i])['nameParts']).split(',')[1][1:-1]
                                    tablealias = ((child['condition'][i])['nameParts']).split(',')[0][1:]
                                    table = tablealias + "." + column
                                else:
                                    column = ((child['condition'][i])['nameParts'])[0]
                                    tablealias = ""
                                    table = column


                                # apply functions and expressions on attribute
                                if len(attr_functions) > 0:
                                    for func in attr_functions:
                                        table = func + "(" + table + ")"
                                    attr_functions.clear()

                                if len(attr_expressions) > 0:
                                    for exp in attr_expressions:
                                        table = table + " " + exp
                                    attr_expressions.clear()

                                attr_output.append(table)
                                if tablealias:
                                    aliases.append(tablealias)
                            else:
                                literaldatatype = child['condition'][i]['dataType']
                                value = child['condition'][i]['value']
                                attr_output.append(value)

                            # if we have mid expression that means we will get two unresoved attributes class
                            if (len(mid_expressions) >= 1):
                                count = count + 1
                                if count == 2:
                                    intermediate_output.append(
                                        attr_output[0] + " " + mid_expressions[0] + " " + attr_output[1])

                                    attr_output.clear()
                                    mid_expressions.clear()
                                    count = 0
                            else:
                                intermediate_output.append(attr_output[0])
                                attr_output.clear()
                    else:
                        if childclass.split('.')[-1] == "EqualTo":
                            mid_expressions.append("=")
                        else:
                            mid_expressions.append(childclass.split('.')[-1])
                else:
                    overall_conditions.append(childclass.split('.')[-1])

                if i + 1 == len(child['condition']):

                    finalaliases = list(set(aliases))

                    _alias_tablenames_new = dict((k.lower(), v) for k, v in _alias_tablenames.items())
                    
                    if finalaliases[0].lower() in _alias_tablenames_new.keys():
                        alias1 = _alias_tablenames_new[finalaliases[0].lower()]
                    else:
                        alias1 = "InlineQuery"
                    if finalaliases[1].lower() in _alias_tablenames_new.keys():
                        alias11 = _alias_tablenames_new[finalaliases[1].lower()]
                    else:
                        alias11 = "InlineQuery"
                    if len(overall_conditions) > 0 and len(intermediate_output) > 1:
                        finalout = alias1 + " " + finalaliases[0] + " " + joinname + " " + alias11 + " " + \
                                   finalaliases[1] + " ON " + " ".join(
                            [x for y in zip(intermediate_output, overall_conditions + [0]) for x in y][:-1])
                    else:
                        finalout = alias1 + " " + finalaliases[0] + " " + joinname + " " + alias11 + " " + \
                                   finalaliases[1] + " ON " + intermediate_output[0]

                    # print(finalout)
                    joinList.append(finalout)

                    overall_conditions.clear()
                    intermediate_output.clear()
                    aliases.clear()
    return joinList
    # print("exit joins info")