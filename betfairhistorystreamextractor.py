import betfairlightweight
from betfairlightweight import StreamListener
import pandas as pd
import os
import bz2
from itertools import repeat
import sys
import json
import ciso8601
import time
from datetime import datetime

def get_file_names(basepath,days_list):
    files_list = []
    # Loop over days to get a list of all files to process
    for day in days_list:
        events = os.listdir(basepath+'/'+day)
        events = [i for i in events if '.DS_Store' not in i] # filter out mac garbage
        for event in events:
            files = os.listdir(basepath+'/'+day+'/'+event)
            for f in files:
                # Here I ignore the event files
                # Check if file one of the futures markets (odd length)
                if len(f) != 15:
                    continue
                files_list.append(basepath+'/'+day+'/'+event+'/'+f)
    return files_list


# Here i've copied the HistoricalStream source from bflw and modified it to read bz2 files
class HistoricalStreamMod:
    """Copy of 'Betfair Stream' for parsing
    historical data. Modified to read BZ2 compressed file
    """

    def __init__(self, file_stream: bz2.BZ2File, listener: StreamListener):
        """
        :param str file_stream: bz2 object
        :param BaseListener listener: Listener object
        """
        self.file_stream = file_stream
        self.listener = listener
        self._running = False

    def start(self) -> None:
        self._running = True
        self._read_loop()

    def stop(self) -> None:
        self._running = False

    def get_generator(self):
        return self._read_loop

    def _read_loop(self):
        self._running = True
        for update in self.file_stream:
            if self.listener.on_data(update) is False:
                # if on_data returns an error stop the stream and raise error
                self.stop()
                raise ListenerError("HISTORICAL", update)
            if not self._running:
                break
            else:
                yield self.listener.snap()
        else:
            # if f has finished, also stop the stream
            self.stop()


# Process the market book from the generator
def get_book_json(gen):
    # print based on seconds to start
    for market_books in gen():
        for market_book in market_books:
            # Since I'm only interested in the win market I've added this to skip 'To Be Placed'
            # Insert other filters here to helps speed things up
            if market_book['marketDefinition']['name'] == 'To Be Placed':
                return
            # Calculate time until the scheduled start
            seconds_to_start = (
                ciso8601.parse_datetime_as_naive(market_book['marketDefinition']['marketTime']) - datetime.utcfromtimestamp(market_book['publishTime']/1000)
            ).total_seconds()
            # Extract book at offset (seconds) before jump
            if seconds_to_start < offset and seconds_to_start > offset-15:
                return market_book


def extract_data_json(mb):
    # Stuff to extract from the book
    date = str(mb['marketDefinition']['marketTime'])[:10]
    if 'venue' in mb['marketDefinition'].keys():
        track = mb['marketDefinition']['venue']
    else:
        track = ''
    if 'name' in mb['marketDefinition'].keys():
        name = mb['marketDefinition']['name']
    else:
        name = ''
    marketId = mb['marketId']
    selectionIds = [i['selectionId'] for i in mb['runners']]
    # Extracting the ladder to depth 5 on both back and lay sides
    p1 = [i['ex']['availableToBack'][0]['price'] if len(i['ex']['availableToBack'])>0 else 1.01 for i in mb['runners']]
    p2 = [i['ex']['availableToBack'][1]['price'] if len(i['ex']['availableToBack'])>1 else 1.01 for i in mb['runners']]
    p3 = [i['ex']['availableToBack'][2]['price'] if len(i['ex']['availableToBack'])>2 else 1.01 for i in mb['runners']]
    p4 = [i['ex']['availableToBack'][3]['price'] if len(i['ex']['availableToBack'])>3 else 1.01 for i in mb['runners']]
    p5 = [i['ex']['availableToBack'][4]['price'] if len(i['ex']['availableToBack'])>4 else 1.01 for i in mb['runners']]
    v1 = [i['ex']['availableToBack'][0]['size'] if len(i['ex']['availableToBack'])>0 else 0 for i in mb['runners']]
    v2 = [i['ex']['availableToBack'][1]['size'] if len(i['ex']['availableToBack'])>1 else 0 for i in mb['runners']]
    v3 = [i['ex']['availableToBack'][2]['size'] if len(i['ex']['availableToBack'])>2 else 0 for i in mb['runners']]
    v4 = [i['ex']['availableToBack'][3]['size'] if len(i['ex']['availableToBack'])>3 else 0 for i in mb['runners']]
    v5 = [i['ex']['availableToBack'][4]['size'] if len(i['ex']['availableToBack'])>4 else 0 for i in mb['runners']]    
    l1 = [i['ex']['availableToLay'][0]['price'] if len(i['ex']['availableToLay'])>0 else 1000 for i in mb['runners']]
    l2 = [i['ex']['availableToLay'][1]['price'] if len(i['ex']['availableToLay'])>1 else 1000 for i in mb['runners']]
    l3 = [i['ex']['availableToLay'][2]['price'] if len(i['ex']['availableToLay'])>2 else 1000 for i in mb['runners']]
    l4 = [i['ex']['availableToLay'][3]['price'] if len(i['ex']['availableToLay'])>3 else 1000 for i in mb['runners']]
    l5 = [i['ex']['availableToLay'][4]['price'] if len(i['ex']['availableToLay'])>4 else 1000 for i in mb['runners']]
    lv1 = [i['ex']['availableToLay'][0]['size'] if len(i['ex']['availableToLay'])>0 else 0 for i in mb['runners']]
    lv2 = [i['ex']['availableToLay'][1]['size'] if len(i['ex']['availableToLay'])>1 else 0 for i in mb['runners']]
    lv3 = [i['ex']['availableToLay'][2]['size'] if len(i['ex']['availableToLay'])>2 else 0 for i in mb['runners']]
    lv4 = [i['ex']['availableToLay'][3]['size'] if len(i['ex']['availableToLay'])>3 else 0 for i in mb['runners']]
    lv5 = [i['ex']['availableToLay'][4]['size'] if len(i['ex']['availableToLay'])>4 else 0 for i in mb['runners']]
    status = [i['status'] for i in mb['runners']]
    # names
    runner_names = dict(zip([i['id'] for i in mb['marketDefinition']['runners']],[i['name'] for i in mb['marketDefinition']['runners']]))
    names = [runner_names[i] for i in selectionIds]
    row = list(zip(repeat(date),repeat(track),repeat(name),repeat(marketId),selectionIds,names,status,p1,p2,p3,p4,p5,v1,v2,v3,v4,v5,l1,l2,l3,l4,l5,lv1,lv2,lv3,lv4,lv5))
    return row


# Folder Structure
# data/2020/11/1
#             /2
#             /3
#            ...

# Configure this to be the path to your data
basepath = './data/2020/11'
days_list = os.listdir(basepath)
days_list = [i for i in days_list if '.DS_Store' not in i] # Filter out mac b.s.

# Configure this to be the number of seconds before the jump you want to grab the price at
offset = 600

# Main Execution starts here
files = get_file_names(basepath,days_list)

data = []
for f_name in files[:50]:
    print(f_name)
    f_pointer = bz2.BZ2File(f_name, 'rb')
    listener = StreamListener(max_latency=None, lightweight=True)
    listener.register_stream(0, "marketSubscription")
    # create historical stream (update directory to your file location)
    stream = HistoricalStreamMod(file_stream=f_pointer, listener=listener)
    gen = stream.get_generator()
    mb = get_book_json(gen)
    if mb:
        race_data = extract_data_json(mb)
        data=data+race_data


df = pd.DataFrame(data,columns=['date','track','name','market_id','selection_id','selection_name','status','p1','p2','p3','p4','p5','v1','v2','v3','v4','v5','l1','l2','l3','l4','l5','lv1','lv2','lv3','lv4','lv5'])

# In case there were multiple files for the same market there may be duplicate prices (or something else went wrong)
# For now just average the prices so there should only be 1 price per selection
dfgrouped = df.groupby(['date','track','name','market_id','selection_id','selection_name','status']).mean().reset_index()

# Label the file based on the latest date in the data
yearmonth = str(dfgrouped.date.max())[:6]

# Save results to file
dfgrouped.to_csv('./output/'+yearmonth+'-'+str(offset)+'.csv',index=False)
