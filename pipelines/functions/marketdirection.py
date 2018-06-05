
import json
import pandas
import quantutils.dataset.pipeline as ppl

def executePipeline(args):
    
    ### PIPELINE ###################        
    ## Process ready for Machine Learning:
    ##
    ## Resample feature units
    ## Crop on time for feature
    ## Resample class units
    ## Crop on time for class
    ## Convert to Feature Sets
    ## Encode the class
    ## Concat the two
    
    data = pandas.read_json(json.dumps(args["data"]), orient='split')
    dataset = args["dataset"]
    features = dataset["features"]
    labels = dataset["labels"]

    ## Resample all to dataset sample unit (to introduce nans in all missing periods)
    featureData = ppl.resample(data, features["sample_unit"]) 

    featureData = ppl.localize(featureData, "UTC", dataset["timezone"])

    print("Features..")

    featureData = ppl.cropTime(featureData, features["start_time"], features["end_time"])

    featureData = ppl.toFeatureSet(featureData, features["periods"])
    
    featureData = ppl.normaliseCandlesticks(featureData, allowZero=True)

    print("Labels..")

    classData = ppl.resample(data, labels["sample_unit"]) 

    classData = ppl.localize(classData, "UTC", dataset["timezone"])

    classData = ppl.cropTime(classData, labels["start_time"], labels["end_time"])

    classData = ppl.toFeatureSet(classData, labels["periods"])

    classData = ppl.encode(classData, labels["encoding"])   
    
    # 23/01/18 - Set index to reflect the predicted time.
    indexedData = ppl.concat(featureData, classData)
    indexedData.index = classData.index

    csvData = indexedData
    #csvData = csvData.append(indexedData)
    # 24/01/18 - Save multiple datasets, one per asset. Provides more flexibility for a range of models.
    # If interleaving of asset data is required then this occurs at training time.
    ########################################

    csvData = ppl.removeNaNs(csvData)

    # 13/07 - Added datetime index, and replaced shuffled data with sorted
    # To avoid a) bias due to correlation between markets, b) bias due to lookahead during training
    #csvData = shuffle(csvData)
    csvData = csvData.sort_index()

    # 26/09 - Split data at training time, rather than pipeline. Also save data to Object Store.
    #csvData_train, csvData_val, csvData_test = split(csvData, train=.6, val=.2, test=.2)

    return json.loads(csvData.to_json(orient='split', date_format="iso"))