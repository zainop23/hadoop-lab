from pyspark import SparkContext

sc=SparkContext("local","Accumulator")

evenCount = sc.accumulator(0)

numbers = sc.parallelize(range(1,11))

numbers.foreach(lambda x: evenCount.add(1) if x%2==0 else None)

print(f"Even count  {evenCount.value}")

# broadcast 

lookup = {
    1 : "one",
    2 : "two",
    3 : "three",
    4 : "four"
}

broadcastVar = sc.broadcast(lookup)
numbers2 = sc.parallelize(range(1,6))
result = numbers2.map(lambda n : broadcastVar.value.get(n,"Not Found"))
print(result.collect())