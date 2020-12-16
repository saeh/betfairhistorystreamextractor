# Betfair History Stream Extractor

Basic python script to scan a betfair history stream bz2 file and extract the price at a particular time before scheduled start

## Notes

Tested on AU Horse Racing stream history files (also works on greyhounds with some mods)

Since I was only interested in the win market i've added code to skip the place markets

Only Extracts 1 price per selection per market. Could easily be extended to grab prices at multiple times

Extracts the price ladder to depth 5. A better way would be to aggregate to price available to return $X.

Heavy lifting on the stream done by Betfairlightweight 

https://github.com/liampauling/betfair

## Instructions

1. Download Stream File from 
https://historicdata.betfair.com/

2. Extract the downloaded tar file and Nav in to the folder containing folders of 'days' 1,2,3,4,5,...,29,30. Copy these days folders into ./data/YYYY/MM/ in this project directory

3. Edit betfairhistorystreamextractor.py and update the variables:

- basepath: this needs to be the path you put everything in from step 2
- offset: the number of seconds before the jump you want the prices from.

4. Make sure dependencies are installed from requirements.txt

- betfairlightweight
- pandas

5. run script with:
```
python betfairhistorystreamextractor.py
```
