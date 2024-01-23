def f(x: int, y: int):
	print("f called")

class myclass:

	def __init__(self, g):
		self.fp1 = g

	def printCall(self):
		self.fp1(1, 2)

obj = myclass(f)
obj.fp1(2, 3)
obj.printCall()