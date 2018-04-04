
import quantutils.dataset.pipeline as ppl

def executePipeline(data, FEATURES, CLASS):
    
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

    ## Resample all to dataset sample unit (to introduce nans in all missing periods)
    featureData = ppl.resample(data, FEATURES["sample_unit"]) 

    featureData = featureData.tz_convert(FEATURES["timezone"])

    print("Features..")

    featureData = ppl.cropTime(featureData, FEATURES["start_time"], FEATURES["end_time"])

    featureData = ppl.toFeatureSet(featureData, FEATURES["periods"])

    featureData = ppl.normaliseCandlesticks(featureData, allowZero=True)

    print("Classes..")

    classData = ppl.resample(data, CLASS["sample_unit"]) 

    classData = classData.tz_convert(CLASS["timezone"])

    classData = ppl.cropTime(classData, CLASS["start_time"], CLASS["end_time"])

    classData = ppl.toFeatureSet(classData, CLASS["periods"])

    classData = ppl.encode(classData, CLASS["encoding"])   
    
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

    return csvData