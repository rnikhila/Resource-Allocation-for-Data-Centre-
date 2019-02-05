"""
This is the project-1 for CSCE 678 Distributed systems and cloud computing.
Input: File contating all commands 

Contributors

Ronak Chaudhary (426006863) 
Rajeswari Nikhila Seemakurty (927001562)
Akshay Gajanan Hasyagar (924002957)
"""

import logging

#logging info
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('aggiestack-log.txt')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)
# stop propagting to root logger
logger.propagate = False

class Instance:
    def __init__(self,instance_name,image_name,flavor_name):
        self.name = instance_name
        self.image = image_name
        self.flavor = flavor_name
        self.rack = "na"
        self.server = "na"
    def allocate_rack_server(self,rack,server):
        self.rack = rack
        self.server = server
        
class Machine:
    def __init__(self,machine_name,rack_name,ip,mem,num_disks,num_cores):
        self.name = machine_name
        self.rack_name = rack_name
        self.ip = ip
        self.mem = mem
        self.num_disks = num_disks
        self.num_cores = num_cores
        self.available_mem = mem
        self.available_disks = num_disks
        self.available_cores = num_cores
    def allocate_space(self,mem,num_disks,num_cores):
        self.available_mem -= mem
        self.available_disks -= num_disks
        self.available_cores -= num_cores
    def deallocate_space(self,mem,num_disks,num_cores):
        self.available_mem += mem
        self.available_disks += num_disks
        self.available_cores += num_cores    

class Rack:
    def __init__(self,rack_name,storagecap):
        self.name = rack_name
        self.storage_cap = storagecap
        self.available_storage = storagecap
        self.images_stored=set()
        self.cur_machines=set()
    def empty_rack(self):
        self.available_storage = self.storage_cap
        self.images_stored=set()
        self.cur_machines=set()
    def update_storage(self,image_size):
        self.available_storage -= image_size

class Flavor:
    def __init__(self,name,mem,num_disks,num_cores):
        self.name = name
        self.mem = mem
        self.num_disks = num_disks
        self.num_cores = num_cores

class Image:
    def __init__(self,name,mem,path):
        self.name = name
        self.mem = mem
        self.path = path
        
        

images = {}
flavors = {}
racks = {}
machines = {}
instances = {}


def read_images(filename):
    logger.info("Reading the images from image config file")
    flag=0
    try:
        file = open(filename, 'r')
    except FileNotFoundError:
        logger.error("File not found")
        return -1
    print ("Reading ",file.readline().strip(), " images lines")
    
    for fi in file.readlines():
        print(fi,end='')
        items=fi.split()
        if len(items)>1: # checking for missing information 
            (name,mem,path) = fi.split()
            image_obj = Image(name,int(mem),path)
            images[name] = image_obj
        else:
            flag = -1
    if (flag < 0):
        print("The config file had invalid lines! Please try again")
        logger.error("Config file has invalid lines")
    file.close()
    

def read_flavors(filename):
    logger.info("Reading the flavors file from flavor config file")
    flag=0
    try:
        file = open(filename, 'r')
    except FileNotFoundError:
        #print("File not found. Enter valid present file")
        logger.error("File not found")
        return -1
    print ("Reading ",file.readline().strip()," flavor lines")
    for fi in file.readlines():
        print(fi,end='')
        items=fi.split()
        if len(items)>1: # checking for missing information 
            (name,mem,num_disks,num_cores) = fi.split()
            flavor_obj = Flavor(name,int(mem),int(num_disks),int(num_cores))
            flavors[name] = flavor_obj
        else:
            flag = -1
    if (flag < 0):
        print("The config file had invalid lines! Please try again")
        logger.error("Config file has invalid lines")
    file.close()
    

def read_hwconfig(filename):
    logger.info("Reading the hardware config from hardware config file")
    global racks
    global machines
    try:
        file = open(filename, 'r')
        logger.info("Opening a file")
    except FileNotFoundError:
        print("File not found. Enter valid present file")
        logger.error("File not found")
        return -1
    print ("Reading no of racks and machines available")
    file_as_list = file.readlines()
    file_stripped = [x.strip() for x in file_as_list]
    #print(file_stripped)
    no_of_racks = int(file_stripped[0])
    for n in range(no_of_racks): #populating racks list
        if(len(file_stripped[1+n].split()) != 2):
            print("The config file had invalid lines! Please try again")
            logger.error("Config file has invalid lines")
            return -1
        (name,storage_cap) = file_stripped[1+n].split()
        rack_obj = Rack(name,int(storage_cap))
        racks[name] = rack_obj 
    i = 1 + no_of_racks #no of machines
    no_of_machines = int(file_stripped[i])
    
    for n in range(1,no_of_machines+1): #populating machines list
        if(len(file_stripped[i+n].split()) != 6):
            print("The config file had invalid lines! Please try again")
            logger.error("Config file has invalid lines")
            return -1
        (name,rack_name,ip,mem,num_disks,num_cores) = file_stripped[i+n].split()
        mach_obj = Machine(name,rack_name,ip,int(mem),int(num_disks),int(num_cores))
        racks[rack_name].cur_machines.add(name)
        machines[name] = mach_obj
    file.close()
    return 1

def evacuate_rack(rack_name):
    logger.info("evavuating the rack")
    inst_to_be_transferred=[]
    rack=racks.pop(rack_name, None)
    if rack:
        machines_to_be_moved = rack.cur_machines
        for mach in machines_to_be_moved:
            for i_k,i_v in instances.items():
                if i_v.server == mach:
                    inst_to_be_transferred.append((i_v.image,i_v.flavor,i_v.name))
        for mach in machines_to_be_moved:
            del machines[mach]            
        for insts in inst_to_be_transferred:
            #imag_,flavo_,nam_=insts
            create_instance_with_rack(*insts)
            #create_instance_with_rack(i_v.image,i_v.flavor,i_v.name)
        rack.empty_rack()
        racks[rack_name]=rack
    else:
        print("rack is not present")
    
    
def remove_machine(machine_name):
    logger.info("removing the machine")
    for key,value in instances.items():
        if value.server  == machine_name:
            print("Cannot delete, instances running on the machine!")
            return 0
    if machine_name in machines:
        mac_details = machines.pop(machine_name)
        #mach_obj = Machine(name,rack,ip,int(mem),int(disks),int(num_cores))
        racks[mac_details.rack_name].cur_machines.remove(machine_name)
        print("Successfully removed", machine_name)
        return 1
    else:
        print(machine_name," not present")
        logger.info(machine_name," machine is not present")
        return 0

def add_machines(mem,disks,num_cores,ip,rack,name):
    logger.info("adding machines to the rack")
    if rack in racks:
        mach_obj = Machine(name,rack,ip,int(mem),int(disks),int(num_cores))
        racks[rack].cur_machines.add(name)
        machines[name] = mach_obj
    else:
        logger.info("The rack is not present")
        print("Rack not present")
    
def create_instance_with_rack(image,flavor,inst_name):
    logger.info("creating instance in the rack with name")
    #for P1-3 checking if rack has the image 
    machines_to_be_considered=[] #these machines will be checked 1st as they are in the rack which already has the image 
    #print(racks.items)
    for key,value in racks.items():
        if flavor in value.images_stored:
            machines_to_be_considered.extend(value.cur_machines)
    return create_instance_with_machines(image,flavor,inst_name,machines_to_be_considered)
            

def create_instance_with_machines(image,flavor,inst_name,mach_list):
    logger.info("creating instance with name")
    image_size = images[image].mem
    #print(image, flavor, inst_name, mach_list)
    for key,value in machines.items():
        if key in mach_list:
            if(value.available_mem >= flavors[flavor].mem and value.available_disks >= flavors[flavor].num_disks and value.available_cores >= flavors[flavor].num_cores):
                #alllocate space in the machine
                value.allocate_space(flavors[flavor].mem,flavors[flavor].num_disks,flavors[flavor].num_cores)
                #creating the instance object
                i_obj = Instance(inst_name,image,flavor)
                #adding instance object to instances
                instances[inst_name] = i_obj
                #double checking if rack has this machine associated with it
                #racks[value.rack_name].cur_machines.append(value.name)
                #P1-C associating image with the rack
                racks[value.rack_name].images_stored.add(image)
                racks[value.rack_name].update_storage(image_size)
                i_obj.allocate_rack_server(value.rack_name,value.name)
                return 1
    create_instance(image,flavor,inst_name)    

def create_instance(image,flavor,inst_name):
    image_size = images[image].mem
    for key,value in machines.items():        
        if(value.available_mem >= flavors[flavor].mem and value.available_disks >= flavors[flavor].num_disks and value.available_cores >= flavors[flavor].num_cores):
            #alllocate space in the machine
            value.allocate_space(flavors[flavor].mem,flavors[flavor].num_disks,flavors[flavor].num_cores)
            #creating the instance object
            i_obj = Instance(inst_name,image,flavor)
            #adding instance object to instances
            instances[inst_name] = i_obj
            #double checking if rack has this machine associated with it
            #racks[value.rack_name].cur_machines.append(value.name)
            #P1-C associating image with the rack
            racks[value.rack_name].images_stored.add(image)
            racks[value.rack_name].update_storage(image_size)
            i_obj.allocate_rack_server(value.rack_name,value.name)
            return 1
    logger.info("unable to create instance due to less memory")
    return 0
      

def can_host(flavor):
    for key,value in machines.items():
        if(value.available_mem >= flavors[flavor].mem and value.available_disks >= flavors[flavor].num_disks and value.available_cores >= flavors[flavor].num_cores):
            return 1
    logger.info("unable to host due to lack of memory")
    return 0

def show_instance():
    print("showing instances for every machine")
    for key,value in instances.items():
        print(key,value.image,value.flavor,value.rack, value.server)
    

def delete_instance(inst_name):
    try:    
        logger.info("deleting the instance")
        server_name = instances[inst_name].server
        flavor_name = instances[inst_name].flavor
        machines[server_name].deallocate_space(flavors[flavor_name].mem,flavors[flavor_name].num_disks,flavors[flavor_name].num_cores)
        del instances[inst_name]
        print("Removed ",inst_name)
    except KeyError:
        print("Instance/Server not found")

def flavor_list():
    logger.info("printing the flavor list")
    #print("<name> <mem> <num-disks> <num-vcpus>")
    for key,value in flavors.items():
        print("<name> ",key," <mem> ",value.mem," <num-disks> ",value.num_disks," <num-vcpus> ",value.num_cores)
        
def image_list():
    logger.info("printing the image list")
    for key,value in racks.items():
        if value.images_stored:
            print("Images locally stored in rack ",value.name,": ",value.images_stored)
    
    #print("<name> <size> <path>")
    for key,value in images.items():
        print("<name> ",key," <size> ",value.mem," <path> ",value.path)

def instance_list():
    logger.info("printing the image list")
    for key,value in instances.items():
        print("<instance name> ",value.name," <image> ",value.image," <flavor> ",value.flavor)
        
def server_list():
    print("<number of racks> ",len(racks))
    for key,value in racks.items():
        print("<rack name> ",value.name," <storage capacity> ",value.storage_cap)
    #print(machines.items())
    for key,value in machines.items():
        #<name> <rack name> <ip> <mem> <num-disks> <num-cores>
        print("<name> ",key," <rack name> ",value.rack_name," <ip> ",value.ip," <mem> ",value.mem," <num-disks> ",value.num_disks," <num-cores> ",value.num_cores)

def admin_show_hw():
    for key,value in machines.items():
        print("<name> ",key,"  <available_mem> ",value.available_mem, " <available_disks> ",value.available_disks, " <available_vcpus> ",value.available_cores )

def admin_show_inst():
    logger.info("Showing the instances")
    for key,value in instances.items():
        print("<instance name> ",value.name," <image> ",value.image," <flavor> ",value.flavor, " <server> ",value.server)

def show_rack_imagecaches(rack_name):
    logger.info("Displaying the images places in rack ")
    for key,value in racks.items():
        if key == rack_name:
            if value.images_stored:
                print("<rack> ",key," <images stored> ",value.images_stored," <available storage> ",value.available_storage,"/",value.storage_cap)
            else:
                print("<rack> ",key," <images stored> no images stored yet ","<available storage> ",value.available_storage,"/",value.storage_cap)





def read_input(line):
    print("____________________________________________________________________________________________________________\n")
    logger.info("Reading the command from the input file")
    print(line)
    words = line.split()
    if words[0] == 'aggiestack':
        if words[1] == 'config':
            if words[2] == '--hardware' or words[2] =='-hardware':
                read_hwconfig(words[3])
            elif words[2] == '--flavors' or words[2] == '-flavors':
                read_flavors(words[3])
            elif words[2] == '--images' or words[2] == '-images':
                read_images(words[3])
            else:
                print("Invalid Command")
        elif words[1] == 'show':
            if words[2] == 'hardware':
                server_list()
            elif words[2] == 'flavors':
                flavor_list()
            elif words[2] == 'images':
                image_list()
            elif words[2] == 'all':
                print("Hardware:")
                server_list()
                print("Flavors:")
                flavor_list()
                print("Images:")
                image_list()
            else:
                print("Invalid Command")
        elif words[1]=='server':
            if words[2] == 'list':
                instance_list()
            if words[2] == 'delete':
                delete_instance(words[3])
            if words[2] == 'create':
                if 'image' in words[3]:
                    image = words[4]
                    if 'flavor' in words[5]:
                        flavor = words[6]
                        name = words[7]
                        create_instance_with_rack(image,flavor,name)
        elif words[1] == 'admin':
            if words[2] == 'remove':
                remove_machine(words[3])
            if words[2] == 'show' and words[3] == 'instances':
                admin_show_inst()
            if words[2] == 'show' and words[3] == 'imagecaches':
                show_rack_imagecaches(words[4]) 
            if words[2] == 'show' and words[3] == 'hardware':
                admin_show_hw()
            if words[2] == 'can_host':
                name = words[3]
                flav = words[4]
                if flav in flavors:
                    if can_host(flav) == 1:
                        print("Yes " + name +" can be hosted" )
                    else:
                        print("No " + name +" can be hosted" )
                else:
                    print("Flavor does not exist")
            if words[2] == 'add':
                mem = words[4]
                disks = words[6]
                num_cores = words[8]
                ip = words[10]
                rack = words [12]
                name = words [13]
                add_machines(mem,disks,num_cores,ip,rack,name)
            if words[2] == 'evacuate':
                evacuate_rack(words[3])
        else:
            print("Invalid Command")
    else:
        print("Invalid Command")


# start
if __name__ == '__main__':
    file_name=input("Enter the filename from where commands will be read: ")
    try:    
        with open(file_name)as f:
            logger.info("Reading the input file containing commands")
            read_data = f.readlines()
            print("Excuting the following commands:")
            logger.info("Executing the commands")
            for lines in read_data:
                read_input(lines)
    except FileNotFoundError:
        logger.info("Tried reading the input file, file not found")
        print("File not found in the current path")
  
