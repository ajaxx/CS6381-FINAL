import os
from time import sleep

class Servers:

    def __init__(self) -> None:
        pass

    def start_servers(self) -> None:
        
        os.system('sudo rm -rf /tmp/kafka-logs')

        while True:
            status = os.popen('sudo kafka/bin/zookeeper-server-stop.sh').read()
            if 'No' in status:
                print(status)
                break
        
        while True:
            status = os.popen('sudo kafka/bin/kafka-server-stop.sh').read()
            if 'No' in status:
                print(status)
                break

        os.system('sudo rm -rf /tmp/kafka-logs')
        os.system('sudo rm -rf output/*')

        os.system('sudo nohup kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties > output/z.txt &')
        sleep(5)
        os.system('sudo nohup kafka/bin/kafka-server-start.sh kafka/config/server.properties > output/k.txt &')

        os.system('sudo kafka/bin/kafka-del-topics.sh')

def main():
    servers = Servers()
    servers.start_servers()

if __name__ == "__main__":
    main()