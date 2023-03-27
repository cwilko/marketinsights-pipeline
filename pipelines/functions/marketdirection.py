
import json
import pandas
import numpy
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
    data.index.names = ['Date_Time'] # To work around lack of index name propogation by marshalling
    dataset = args["dataset"]
    features = dataset["features"]
    labels = dataset["labels"]

    data = ppl.localize(data, "UTC", dataset["timezone"])
    
    ## Resample all to dataset sample unit (to introduce nans in all missing periods)
    resampledData = ppl.resample(data, features["sample_unit"]) 

    featureData = ppl.cropTime(resampledData, features["start_time"], features["end_time"])
    
    # Quit if partial feature data
    if len(featureData) % features["periods"]:
        return json.loads(pandas.DataFrame().to_json(orient='split', date_format="iso"))
        
    featureData = ppl.toFeatureSet(featureData, features["periods"])        
        
    featureData = ppl.normaliseCandlesticks(featureData, allowZero=True)
    
    print("Features: " + str(len(featureData)))

    if labels["sample_unit"] != features["sample_unit"]:
        resampledData = ppl.resample(data, labels["sample_unit"]) 
    
    partialData = False
    i = 0
    freq = resampledData.index.freq
    while True:
        # Extract labels
        classData = ppl.cropTime(resampledData, labels["start_time"], labels["end_time"])
        classData = ppl.toFeatureSet(classData, labels["periods"])
        
        print("Labels: " + str(len(classData)))
        
        if len(classData) == len(featureData):
            break
            
        if len(classData) > len(featureData):
            return json.loads(pandas.DataFrame().to_json(orient='split', date_format="iso"))
        
        if i>25:
            raise Exception(f"ERROR: Number of features and labels do not match. Features: {len(featureData)}, Labels: {len(classData)}")
            
        # We are likely in a situation where we have n features but n-1 labels.
        # Append period to classData until we have label length same as features.
        resampledData = pandas.concat([resampledData, pandas.DataFrame(index=[resampledData.index[-1] + freq])])
        partialData = True
        i = i + 1
    
    classData = ppl.encode(classData, labels["encoding"])
    if partialData: # Set the label to -1 to avoid nan removal (hack)
        classData.iloc[-1] = -1
                    
    # 23/01/18 - Set index to reflect the predicted time.
    indexedData = ppl.concat(featureData, classData)
    indexedData.index = classData.index

    #csvData = indexedData
    #csvData = csvData.append(indexedData)
    # 24/01/18 - Save multiple datasets, one per asset. Provides more flexibility for a range of models.
    # If interleaving of asset data is required then this occurs at training time.
    ########################################

    indexedData = ppl.removeNaNs(indexedData)
    
    if partialData: # Reset the final label back to nan (hack)
        indexedData.values[-1][-1] = numpy.nan

    # 13/07 - Added datetime index, and replaced shuffled data with sorted
    # To avoid a) bias due to correlation between markets, b) bias due to lookahead during training
    #csvData = shuffle(csvData)
    indexedData = indexedData.sort_index()

    # 26/09 - Split data at training time, rather than pipeline. Also save data to Object Store.
    #csvData_train, csvData_val, csvData_test = split(csvData, train=.6, val=.2, test=.2)
    
    # 21/03/23 - Convert to float32. Better suited to training data. Fit more in memory and faster.
    # csvData = csvData.astype(numpy.float32) - Actually - leave this to training for more flex.

    return json.loads(indexedData.to_json(orient='split', date_format="iso"))
