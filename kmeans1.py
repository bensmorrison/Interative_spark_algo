import sys
import math
from pyspark import SparkContext

# declarations
sc = SparkContext()
filepath = sys.argv[1]
filename=filepath
K = 5
convergeDist = 0.1
distance=0

def closestPoint(point, array):
        index = 0
    best

        for i in array:
            if distanceSquared(point,array[i]) < best:
                    best = distanceSquared(point,array[i])
                    index = i

        return index

def addPoints(x,y):
    one = x[0]+y[0]
    two = x[1]+y[1]
    return [one, two]
    
def distanceSquared(x,y):
    one = x[0]-y[0]
    two = x[1]-y[1]
    return (math.pow(one, 2) + math.pow(two, 2))
        
mycorddata = sc.textFile(filename)

fields = mycorddata.map(lambda line: line.split(",")).persist() #del. by ,

mapfields = fields.map(lambda fields: [float(fields[3]),float(fields[4])]).persist() #fields 4 and 5

filter_fields = mapfields.filter(lambda point: [point[0] != 0, point[1] != 0]).persist() # filter out (0,0)

kPoints = filter_fields.takeSample(False, K, 42) #given

while distance > convergeDist:
    pointindex =points.map(lambda point : (closestPoint(point, kPoints), (point, 1)))

    pointcord = pointindex.reduceByKey(lambda (point1,one),(point2,two): (addPoints(point1,point2),one+two))

    newkpoints =pointcord.map(lambda (index,(total,n)): (index,[total[0]/n,total[1]/n]))

    for (index, point) in newkpoints:
        distance += distanceSquared(kPoints[index],point)
        kPoints[index] = point
        
print "Final k center points: " + str(kPoints)
