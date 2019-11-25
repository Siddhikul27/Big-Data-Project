from mrjob.job import MRJob
class ProjectMapReduce(MRJob):

    def mapper(self, _, line):
        prevailing_wages = []
        #f = open(r"prevailing_wage.txt", "r")
        #for line in f:
        x = line.rstrip("\n")
        #prevailing_wages.append(float(x))
        yield x,1

    def reducer(self, x, counts):
        sumwage = 0.0
        number=0.0
        average= 0.0
    
        for wages in counts:
            y = float(x)
            sumwage += y
            number+=1
            average = sumwage/number
            #min_wage = next(counts)
            #max_wage = min_wage
            #for cal in x:
                #min_wage = min(cal,float(min_wage))
                #max_wage = max(cal,float(max_wage))
            
        yield x,(number,sumwage,average)
        #yield x,(max(sumwage),min(sumwage))
        #yield x,(min_wage,max_wage)
        #yield x,(minimum_value, maximum_value)
        
if __name__ == '__main__':
    ProjectMapReduce.run()