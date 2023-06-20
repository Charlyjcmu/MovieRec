#Code to only get rating data
from kafka import KafkaConsumer,TopicPartition
from subprocess import Popen, PIPE
import csv
import os
import requests
import pickle
import DataStreaming.OffsetsRate as Offsets

#checks input is number and greater than 0
def checkInput(input,defVal,valName):
    try:
        val = int(input)
        if val <=0:
            print(f"using default {defVal} for {valName}")
            val = defVal
    except ValueError:
        print(f"using default {defVal} for {valName}")
        val = defVal
    return val

#Creates Data folder if doesn't exist. 
def folderInit():
    #is file called Data exists
    if not os.path.isdir("Data"):
        if os.path.exists("Data"):
            raise RuntimeError("File called Data already exists")
        else:
            #make Data directory if not exists
            os.makedirs("Data")

#initialize file objects and file headers if new
def fileInit(filePath,header):
    file = open(filePath, "a")
    if not header:
        return [file]
    csvWriter = csv.writer(file)
    if os.path.getsize(filePath) == 0:
        csvWriter.writerow(header)
    return [file,csvWriter]

#initialize the consumer after checking port and bootstrap
def consumeInit():
    #check if port is used
    p1 = Popen(['lsof', f'-ti:9092'], stdout=PIPE)
    output = p1.communicate()[0]
    if not output.decode():
        raise RuntimeError("local tunnel not open")
    
    # Create a consumer to read data from kafka (initialize)
    c = KafkaConsumer(
        "movielog14",
        bootstrap_servers=[f"localhost:9092"],
        # Read from the start of the topic; Default is latest
        auto_offset_reset='earliest') #ordering of messages
    if not c.bootstrap_connected():
        raise RuntimeError("Bootstrap not connected")
    return c

#initialize the initial offsets
def offsetInit(c,partitions,pOffCurr):
    initOffset = c.beginning_offsets(partitions)
    #if curr offset too far back, then reset offset to beginning
    if (pOffCurr[0] < initOffset[partitions[0]]):
        pOffCurr[0] = initOffset[partitions[0]]
    if (pOffCurr[1] < initOffset[partitions[1]]):
        pOffCurr[1] = initOffset[partitions[1]]
    #start offset from previous ending
    c.seek(partitions[0], pOffCurr[0])
    c.seek(partitions[1], pOffCurr[1])
    return pOffCurr

#message checking and writing
def writeMsg(msgVal,mpgWr,rateWr,recsWr,movieList):
    mpgStr = "GET /data/m/"
    rateStr = "GET /rate/"
    recStr = "recommendation request 17645-team14.isri.cmu.edu:8082, status 200, result:"
    URL = "http://128.2.204.215:8080/movie/"

    msgSplit = msgVal.split(",",4)
    try:
        if (rateStr in msgVal):
            if (msgSplit[1].isdigit() and 0<int(msgSplit[1]) and int(msgSplit[1])<1000001): #check id
                if(msgSplit[2][-1].isdigit() and 0<int(msgSplit[2][-1]) and int(msgSplit[2][-1])< 6): #check rate
                    movieName = msgSplit[2][10:-2] #check movie exists
                    if(movieName in movieList):
                        rateWr.writerow(msgSplit)
                        return
                    elif(requests.get(URL + movieName).status_code == 200):
                        movieList.append(movieName)
                        rateWr.writerow(msgSplit)
                        return
                    return
        return
    except ValueError:
        print(msgVal)

#closing functions
def closeFiles(mpgCsv,recsCsv,rateCsv):
    mpgCsv.close()
    recsCsv.close()
    rateCsv.close()

#saving current Offsets
def writeOffsets(pOff):
    with open("DataStreaming/OffsetsRate.py", "w") as myfile:
        myfile.write("PART0OFF=" + "%d\n" % pOff[0])
        myfile.write("PART1OFF=" +"%d\n" % pOff[1])

#gets data of max rounds with each round of batch_size
#returns -1 on error, and messages read on success 
def getRateData(RoundsInput=5000, batchInput=20000):
    # initialize the inputs
    maxRounds = checkInput(RoundsInput, 5000, "max rounds")
    batch_size = checkInput(batchInput, 20000, "batch size")

    # create the folder
    try:
        folderInit()
    except RuntimeError as error:
        print(repr(error))
        return -1

    # open files for writing
    try:
        mpg = fileInit("Data/kafka_mpg.csv", ["time", "user id", "movie id"])
        mpgCsv = mpg[0]
        mpgWriter = mpg[1]
        recs = fileInit("Data/kafka_recs.csv",["time","user id","ip","status","recs","ms"])
        recsCsv = recs[0]
        recsWriter = recs[1]
        rate = fileInit("Data/kafka_rate.csv", ["time", "user id", "movie id"])
        rateCsv = rate[0]
        rateWriter = rate[1]
        if (os.path.isfile("Data/movieslist.txt")):
            with open("Data/movieslist.txt", "rb") as fp:   #Pickling
                moviels = pickle.load(fp)
        else:
            moviels = []
        #print(moviels)
    except IOError:
        print("Files not opened correctly")
        return -1
    
    # Create a consumer to read data from kafka (initialize)
    try:
        consumer = consumeInit()
    except RuntimeError as error:
        print(repr(error))
        return -1
    parts = [TopicPartition("movielog14", 0), TopicPartition("movielog14", 1)]
    endOff = consumer.end_offsets(parts)
    pOffCurr = offsetInit(consumer, parts, [Offsets.PART0OFF, Offsets.PART1OFF])
    msgRd = 0

    # either loop maxRounds or until reached the end indices
    for count in range(maxRounds):
        if not (count % 50):
            print(f'Progress: {pOffCurr[0]}, {pOffCurr[1]}')
            print(f'Goal: {endOff[parts[0]]}, {endOff[parts[1]]}')
            print(f'Mandatory stop in {maxRounds - count} rounds')
        records = consumer.poll(10000, batch_size)
        # make sure to check partition
        if (parts[0] in records):
            record0 = records[parts[0]]
            msgRd += len(record0)
            # write each message
            for message in record0:
                msgVal = message.value.decode()
                writeMsg(msgVal,mpgWriter,rateWriter,recsWriter, moviels)
            # adjust offset
            pOffCurr[0] = message.offset + 1
        if (parts[1] in records):
            record1 = records[parts[1]]
            msgRd += len(record1)
            for message in record1:
                msgVal = message.value.decode()
                writeMsg(msgVal,mpgWriter,rateWriter,recsWriter,moviels)
            pOffCurr[1] = message.offset + 1
        if (pOffCurr[0] >= endOff[parts[0]] and pOffCurr[1] >= endOff[parts[1]]):
            break
    
    closeFiles(mpgCsv, recsCsv, rateCsv)
    #print(moviels)
    with open("Data/movieslist.txt", "wb") as fp:   #Pickling
        pickle.dump(moviels, fp)
    # save new Offsets
    writeOffsets(pOffCurr)
    return msgRd

#msgCount = getRateData(1,20000)
#5000 rounds of batch 20000 takes around 20 minutes
#msgCount = getData(10,20000)
#print(f"read {msgCount} values")
