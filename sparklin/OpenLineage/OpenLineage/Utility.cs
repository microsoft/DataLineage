using System;
using System.Collections.Generic;
using System.Text;

namespace OpenLineage
{
    class Utility
    {
        public static String getQualifierName(String notebookName)
        {
            if (notebookName != null && notebookName.IndexOf(Constant.GAT, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return Constant.GAT.ToUpper();
            }
            else if (notebookName != null && notebookName.IndexOf(Constant.HRSI, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return Constant.HRSI.ToUpper();
            }
            else if (notebookName != null && notebookName.IndexOf(Constant.LEARNING, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return Constant.LEARNING.ToUpper();
            }
            else if (notebookName != null && notebookName.IndexOf(Constant.HCM, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return Constant.HCM.ToUpper();
            }
            else if (notebookName != null && notebookName.IndexOf(Constant.ULTP, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return Constant.ULTP.ToUpper();
            }
            else
            {
                return notebookName.Substring(0, notebookName.LastIndexOf("_")).ToUpper();
            }

        }

        public static void main()
        {
            Console.WriteLine(Utility.getQualifierName("sql_test_data_lineage_pool_1650268838"));
        }
    }
}
