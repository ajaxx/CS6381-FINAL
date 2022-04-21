import sub
from Utilities import alpaca

import sys
from time import sleep

def main():
    
    ticker = sys.argv[1]

    a = alpaca.Alpaca()
    s = sub.Sub(ticker)

    s.recieve()



if __name__ == '__main__':
    main()