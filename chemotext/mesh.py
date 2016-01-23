import json
import logging
import os
import sys
import xml.etree.cElementTree as et

FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
#logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__file__)

class MeSH (object):
    def __init__(self, file_name):
        self.disease_mesh_prefix = "C"
        self.chemical_mesh_prefix = "D"
        self.protein_mesh_prefix = "D12"
        
        self.proteins = []
        self.chemicals = []
        self.diseases = []

        dirname = os.path.dirname (file_name)
        self.mesh_store_json = os.path.join (dirname, "MeSH.json")
        #print "----------> {0}".format (self.mesh_store_json)
        
        if os.path.exists (self.mesh_store_json):
            with open (self.mesh_store_json) as stream:
                db = json.loads (stream.read ())
                self.proteins = db['proteins']
                self.chemicals = db['chemicals']
                self.diseases = db['diseases']
        else:
            self.parse (file_name)

        self.proteins = [ p.lower () for p in self.proteins ]
        self.chemicals = [ c.lower () for c in self.chemicals ]
        self.diseases = [ d.lower () for d in self.diseases ]
            
    def add_chemical (self, name):
        self.chemicals.append (name)
    def add_protein (self, name):
        self.proteins.append (name)
    def add_disease (self, name):
        self.diseases.append (name)
    def parse (self, file_name):
        with open (file_name) as stream:
            tree = et.parse (stream)
            descriptors = tree.findall ("./DescriptorRecord")
            for d in descriptors:
                element = d.find ("./DescriptorName/String")
                tn_list = d.findall ("./TreeNumberList/TreeNumber")
                if tn_list is not None:
                    tree_numbers = [ n.text for n in tn_list ]
                    for n in tree_numbers:
                        if n.startswith (self.protein_mesh_prefix):
                            self.proteins.append (element.text)
                            break
                        elif n.startswith (self.disease_mesh_prefix):
                            self.diseases.append (element.text)
                            break
                        elif n.startswith (self.chemical_mesh_prefix):
                            self.chemicals.append (element.text)
                            break
        ''' Cache as JSON '''
        with open (self.mesh_store_json, "w") as output:
            output.write (json.dumps ({
                "proteins" : self.proteins,
                "chemicals" : self.chemicals,
                "diseases" : self.diseases
            }, sort_keys=True, indent=2))

def main ():
    file_name = sys.argv [1]
    mesh = MeSH (file_name)
    for p in mesh.proteins:
        logger.info ("protein {0}".format (p))
    for p in mesh.chemicals:
        logger.info ("chemical {0}".format (p))
    for p in mesh.diseases:
        logger.info ("disease {0}".format (p))

#main ()
