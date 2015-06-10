
#simple methods
#===============================================================================
def sum_if_all_int(v_list):
    s = 0
    for l in v_list:
        try:
            s+=int(l)
        except:
            return -1
    return s


def avg(v_list):
    summa = sum_if_all_int(v_list)
    if summa ==-1:
        return summa
    else:
        if summa%len(v_list) !=0:
            avg = summa/float(len(v_list))
        else:
            avg = summa/len(v_list)
    return avg


def summa(v_list):
    summa = sum_if_all_int(v_list)
    return summa

def amount_output(v_list): #how many parts  
    iterlength = len(v_list)/10 if len(v_list)%10==0 else len(v_list)/10+1
    return iterlength

def cut_list(v_list): #parts
    st = 0
    end = 10
    for i in xrange(amount_output(v_list)):
        #print v_list[st:end]
        yield v_list[st:end]
        st=end
        end+=10
#===============================================================================
class DataFromCsv:
    def __init__(self, column_names, data_list):
        self.column_names = column_names
        self.data_list = data_list 

#===============================================================================
class CsvFile:
    def __init__(self, filepath, dataset_name = None):
        self.filepath = filepath
        self.dataset_name = dataset_name
       
    
    def get_name(self):
        slash_symbol = self.filepath.find('\\')
        #no slash
        if slash_symbol ==-1:
            file_name_with_ext = self.filepath.split('.')
        #slash
        else:
            file_path_list = self.filepath.split('\\')
            file_name_with_ext = file_path_list[len(file_path_list)-1].split('.')
        file_name =file_name_with_ext[0]    
        return file_name
        
    
    
    def virtual_label(self):
        iter_count = 0
        data_list = []
        for line in open(self.filepath):
            if iter_count == 0:
                #make attrs
                line = line.rstrip() #del \n
                pl_attrs = line.split(';')
                column_names = pl_attrs
            else:
                line = line.rstrip() #del \n
                pl_attrs = line.split(';')
                pl = dict(zip(column_names, pl_attrs))
                data_list.append(pl)
            iter_count+=1 #+iter count
        data_csv = DataFromCsv(column_names, data_list)
        return data_csv

#=========================================================================================
class WhereExpression:
    def __init__(self, param):
        self.param = param
        self.possible_operations = ['<=', '>=', '<', '>', '=']
    def parse_where(self):
        for pos_op in self.possible_operations:
            if self.param.find(pos_op)!=-1:
                param_splitted = self.param.split(pos_op)
                self.field = param_splitted[0]
                self.value = param_splitted[1].replace("'","")
                self.operation = pos_op
                return 1    

#=========================================================================================
#simple sql expression
class SqlExpression:
    def __init__(self, expression,we,data_set_from_string):
        self.expression = expression
        self.we = we
        self.data_set_from_string = data_set_from_string



    def create_header_attr(self):
        self.header_len = [len(sh)+2 for sh in self.header]
        self.header_dict = dict(zip(self.header, self.header_len))





    def execute_we(self):
        new_result = []
        for r in self.result:
            if self.we.operation == '=':
                if (r[self.we.field])==(self.we.value):
                    new_result.append(r)
            elif self.we.operation == '<':
                if int(r[self.we.field])<int(self.we.value):
                    new_result.append(r)
            
            elif self.we.operation == '>':
                if int(r[self.we.field])>int(self.we.value):
                    new_result.append(r)

            elif self.we.operation == '<=':
                if int(r[self.we.field])<=int(self.we.value):
                    new_result.append(r)
            elif self.we.operation == '>=':
                if int(r[self.we.field])>=int(self.we.value):
                    new_result.append(r)      
        self.result = new_result



    #for aggreagate
    def aggregate(self, agr_type, data):
        split_exp = self.expression.split('(')
        field = split_exp[1][:-1]
        self.header = [agr_type+'('+field+')']
        self.create_header_attr()

        #all
        t_result = data[self.data_set_from_string].data_list
        #temp list
        result_list = []
        for temp_rez_dict in t_result:
            try:
                val = int(temp_rez_dict[field])
            except:
                val = temp_rez_dict[field]
            result_list.append(val)
        if agr_type == 'min':
            self.result = {self.header[0]:min(result_list)}
        elif agr_type == 'max':
            self.result = {self.header[0]:max(result_list)}
        elif agr_type == 'avg':
            self.result = {self.header[0]:avg(result_list)}
        elif agr_type == 'sum':
            self.result = {self.header[0]:summa(result_list)}
        elif agr_type == 'count':
            self.result = {self.header[0]:len(result_list)}
        self.res_type = 'aggregation'



    def execute_sql(self,data): #{'p': <models.DataFromCsv instance at 0x01831D50>}


        self.data = data
        if self.expression == '*': 
            self.header = data[self.data_set_from_string].column_names
            self.create_header_attr()
            self.result = data[self.data_set_from_string].data_list
            self.res_type = 'field'
            
            #executing we
            if self.we != None:
                self.execute_we()

        #min, max etc.
        elif self.expression.startswith('min('):
            self.aggregate('min',data)

        elif self.expression.startswith('max('):
            self.aggregate('max', data)

        elif self.expression.startswith('avg('):
            self.aggregate('avg', data)

        elif self.expression.startswith('sum('):
            self.aggregate('sum', data)

        elif self.expression.startswith('count('):
            self.aggregate('count', data)


        else: #case with fields
            if not( set(self.expression.split(',')) <= set(data[self.data_set_from_string].column_names)):
                print 'this field/s does not exist'
                return -1
            else:
                self.header = self.expression.split(',')
                self.create_header_attr()
                self.result = data[self.data_set_from_string].data_list
                self.res_type = 'field'
                #executing we
                if self.we != None:
                    self.execute_we()



    #draw_header
    def draw_header(self):
        print '+' + ((sum(self.header_len)) + (len(self.header))-1) *'-' + '+' , '\n' , '|',
        for h in self.header:
            print h,'|',
        print
        print '+'+ ((sum(self.header_len)) + (len(self.header))-1) *'-' +'+'        




    def create_table(self):


        #make_data

        if self.res_type == 'aggregation' and self.result.values()[0] == -1:
            print 'the field you specified does not support this aggregation type (the field must be integer type)'
        else:
            if self.res_type == 'aggregation' and self.result.values()[0] != -1:
                self.draw_header()

        
        #if field
        if self.res_type =='field':
            #print '1'
            ao = amount_output(self.result)
            ao_iter = 1
            for cutted_result in cut_list(self.result):
                self.draw_header()

                for r_dict in cutted_result:
                    for sh in self.header:
                        for col_name,value in r_dict.iteritems():
                            if sh==col_name:
                                #make row
                                #print '%s%s' % (value(self.header_dict[sh]), '|'),
                                if self.header[0] == sh:
                                    print '%s%s%s' % ('|', value.center(self.header_dict[sh]), '|'),
                                else:
                                    print '%s%s' % (value.center(self.header_dict[sh]-1), '|'),
                    print

                print '+'+ ((sum(self.header_len)) + (len(self.header))-1) *'-' +'+'

                #count objects notifications
                if ao>1:
                    print len(cutted_result), 'records showed'
                    #print 'amount of all records is', len(self.result)
                print len(self.result), 'records found' 

                if ao_iter < ao:
                    print ('(press ENTER to see next)')
                    raw_input() 
                ao_iter+=1


        #if aggregation
        elif self.res_type == 'aggregation':
            if self.result.values()[0] == -1:
                pass
            else:

                for sh in self.header: 
                    if sh == self.result.keys()[0]:
                        print '%s%s%s' % ('|', str(self.result[sh]).center(self.header_dict[sh]), '|'),
                print
                print '+'+ ((sum(self.header_len)) + (len(self.header))-1) *'-' +'+'

        if len(self.result)==0:
            print '0 records found'

#===========================================================================================
