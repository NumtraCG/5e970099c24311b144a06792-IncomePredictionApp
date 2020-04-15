import json
import Connectors
import Transformations
import AutoML
try:
    IncomePredictionApp_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e970099c24311b144a06793", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    IncomePredictionApp_AutoFE = Transformations.TransformationMain.run(["5e970099c24311b144a06793"], {"5e970099c24311b144a06793": IncomePredictionApp_DBFS}, "5e970099c24311b144a06794", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "333", "mean": "", "stddev": "", "min": "AGRICULTURAL", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "333", "mean": "194.2", "stddev": "575.22", "min": "0", "max": "5586", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "333", "mean": "1002.02", "stddev": "379.73", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "333", "mean": "176.85", "stddev": "683.04", "min": "0", "max": "9933", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "333", "mean": "790.21", "stddev": "278.28", "min": "1025", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {
                                                                        "transformationsData": {}, "feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "333", "mean": "371.1", "stddev": "1100.4", "min": "0", "max": "13894", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "333", "mean": "901.07", "stddev": "333.15", "min": "1000", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "333", "mean": "166.0", "stddev": "96.27", "min": "0.0", "max": "332.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "333", "mean": "28.91", "stddev": "42.77", "min": "0.0", "max": "138.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "333", "mean": "19.36", "stddev": "33.05", "min": "0.0", "max": "113.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "333", "mean": "46.25", "stddev": "56.53", "min": "0.0", "max": "174.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionRegression(IncomePredictionApp_AutoFE, [
                              "Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "All_weekly")

except Exception as ex:
    print(ex)
