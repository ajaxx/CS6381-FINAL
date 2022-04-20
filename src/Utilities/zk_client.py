from kazoo.client import KazooClient

class ZKClient:

    host_ip: str
    host_port: str

    client: KazooClient

    def __init__(self, host_ip: str, host_port: str) -> None:
        """ Initializes the ZKClient class and establishs a 
        connection to the host node

        Arguements:
            host_ip (str): The IP address of the host node
            host_port (str): The port of the host node
        
        Retruns:
            None
        """
        
        self.host_ip = host_ip
        self.host_port = host_port

        self.client = KazooClient(hosts=f"{host_ip}:{host_port}")         # Can we pass in multiple hosts and does that automatically handle the ensamble server?
        self.client.start()
    
    def create_znode(self, key: str, value: str, ephemeral: bool = True):
        """ Creates a znode with the given key and value, and dictates
        whether or not the znode is ephemeral

        Arguements:
            key (str): The key of the znode
            value (str): The value of the znode

        Returns:
            None
        """

        if self.client.exists(f"/{key}"):
            self.client.delete(f"/{key}")
            
        print(f"Creating node of key: {key} - value: {value}")
        self.client.create(f"/{key}", value=str.encode(value), ephemeral=ephemeral, makepath=True)
    

    def create_broker_znode(self, key: str, value: str):
        """ Creates a znode with the given key and value

        Arguements:
            key (str): The key of the znode
            value (str): The value of the znode 

        Returns:
            None
        """
        
        print(f"Creating node of key: {key} - value: {value}")
        self.client.create(f"/BROKER/{key}_", value=str.encode(value), ephemeral=True, sequence=True, makepath=True)
    
    def create_balancer_znode(self, key: str, value: str):
        """ Creates a znode with the given key and value under the
        BALANCER znode
        
        Arguements:
            key (str): The key of the znode
            value (str): The value of the znode

        Returns:
            None
        """

        self.client.create(f"/BALANCER/{key}_", value=str.encode(value), ephemeral=True, sequence=True, makepath=True)
            
    def get_children(self, child):
        """ Gets the children of the given znode

        Arguements:
            child (str): The child of the znode

        Returns:
            children (list): The children of the znode
        """

        if self.client.exists(f"/{child}"):
            return self.client.get_children(f'/{child}')
        
        return []

    def set_value(self, key: str, value: str):
        """ Sets the value of the given znode
        
        Arguements:
            key (str): The key of the znode
            
        Returns:
            None
        """
        
        self.client.set(f"/{key}", str.encode(value))

    def get_value(self, znode: str):
        """ Gets the value of the given znode
        
        Arguements:
            znode (str): The znode to get the value of
            
        Returns:
            value (str): The value of the znode
        """
        
        if self.client.exists(f"/{znode}"):
            value, stat = self.client.get(f"/{znode}")              # Need to do locking call
            return value.decode("utf-8")

        else:
            return ""
