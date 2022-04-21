import pub
from Utilities import alpaca

import sys
from time import sleep

def main():
    
    ticker = sys.argv[1]
    data_type = sys.argv[2]

    a = alpaca.Alpaca()
    p = pub.Pub()

    data = a.get_ticket_data(ticker, data_type)

    for data_point in data:
        p.publish(ticker, data_point)
        sleep(1)



if __name__ == '__main__':
    main()