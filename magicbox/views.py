#!/usr/bin/env python
from models import * 
def magicbox():
    #for param in sys.argv:
    #    print
    

    virt_label_dict = {}# 
    dataset_to_file_dict = {}

    quit_param = True
    while quit_param:

        
        try:
            #command open
            command_line = raw_input('>')
            command = command_line.split()
            if command[0] == 'open':  #check number of params
                if len(command) == 4 and command[2] =='as':
                    try:
                        f = open(command[1], 'r')
                        f.close()
                    except:
                        print 'enter valid file name'
                        continue
                    csv_file = CsvFile(command[1], command[3])
                else:
                    try:
                        f = open(command[1], 'r')
                        f.close()
                    except:
                        print 'enter valid file name'
                        continue
                    csv_file = CsvFile(command[1])
                
               
                #associated  name
                if csv_file.dataset_name == None:
                    assoc_name = csv_file.get_name()
                else:
                    assoc_name = csv_file.dataset_name
                
                dataset_to_file_dict[assoc_name] = csv_file


                
             
            elif command[0] == 'select':
                if dataset_to_file_dict == {}:
                    print 'no dataset was defined. define dataset at first'
                    continue
                
                #select expr
                after_select =  command[1:]
                after_select = ''.join(after_select)
                if after_select.find('from')==-1:
                    print 'check syntax of command'
                    continue
                expr = after_select.split('from')
                
                #data_set
                after_from =  command_line.split('from')[1]
                data_set_from_string = after_from.split('where')[0].strip()
                #check
                
                if data_set_from_string not in dataset_to_file_dict.keys():
                    print 'check the name of dataset'
                    continue
                
                #where
                try:
                    where =  command_line.split('where')[1]
                    where = where.replace(' ','')
                    we = WhereExpression(where)
                    we.parse_where()
                except:
                    we=None

                se = SqlExpression(expr[0], we, data_set_from_string) #1st param - field/fields/*, 2nd param where expression
                #take date from file
                csv_file_obj = dataset_to_file_dict[data_set_from_string]
                data_list = csv_file_obj.virtual_label()
                virt_label_dict[data_set_from_string] = data_list


                #
                t = se.execute_sql(virt_label_dict) 
                if t!=-1:
                    se.create_table()
                    del virt_label_dict[data_set_from_string]

            
            #close
            elif command[0] == 'close':
                del dataset_to_file_dict[command[1]]


            elif command[0] == 'quit':
                quit_param = False
                print 'bye'
            

            else:
                print 'check syntax of command'
        except:
            print 'check syntax of command'

