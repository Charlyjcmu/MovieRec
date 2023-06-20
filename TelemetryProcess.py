from DataStreaming.kafkaPipe import getRateData
from DataCleaning.raw_mpg_preprocess import getmpgDf
from DataCleaning.compress_mpg import sparkCompress

#getRateData(20000,20000)
#getmpgDf() 
sparkCompress()
