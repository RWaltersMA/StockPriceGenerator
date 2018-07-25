import threading
days=365
numofthreads=20
numbertoprocess=int(days/numofthreads)
threads=[]
def worker(g,process):
    startday=g*process
    if g==(numofthreads-1):
         endday=((g+1)*process)
    else:
        endday=((g+1)*process)-1

    print 'Worker: ' + str(g) + ', Working on days ' + str(startday) + ' to ' + str(endday)

for i in range(0,numofthreads):
    t=threading.Thread(target=worker, args=(i,numbertoprocess,))
    threads.append(t)
    t.start()
