import random

def generateKpoints(k, rnge):
    Ks= []

    while len(Ks)<k:
        val= random.uniform(rnge[0],rnge[1])
        if val not in Ks:
            Ks.append(val)


    return Ks

def getKsets(Xs, Ks):
    k= len(Ks)
    Ss= []
    for i in range(k):
        Ss.append([])

    for X in Xs:
        mn= float('inf')
        idx= k
        for i in range(k):
            if(pow(X-Ks[i],2)<mn):
                mn= X-Ks[i]
                idx= i
        Ss[idx].append(X)

    return Ss


def getKcentroids(sets, oldCentroids):
    Ks= []

    maxcap= float('-inf')
    mincap= float('inf')

    for s in sets:
        for x in s:
            if x>maxcap:
                maxcap= x
            if x<mincap:
                mincap= x

    for i in range(len(sets)):
        K= int()
        if len(sets[i])==0:
            K= oldCentroids[i]
        else:
            sumx= 0
            for x in sets[i]:
                sumx+= x

            K= sumx/len(sets[i])

        Ks.append(K)

    return Ks

def foo(sets, centroids):
    result= 0
    for i in range(len(sets)):
        for j in sets[i]:
            result+= pow(centroids[i]-j,2)

    return result


def main():
    n= int(input("Enter no. of points: "))
    Xs= []
    if n<=10:
        for a in range(n):
            x= float(input(f"Enter point no. {a}: "))
            Xs.append(x)
    else:
        Xs= generateKpoints(n, [-100000, 100000])


    k= int(input("Enter k: "))
    Ks= generateKpoints(k, [min(Xs),max(Xs)])
    for K in Ks:
        print(K)

    Ss= getKsets(Xs, Ks)
    print(Ss)
    newKs= getKcentroids(Ss, Ks)
    print(newKs)

    for i in range(50):
        oldSs= Ss
        Ss= getKsets(Xs, newKs)
#        print(Ss)
        oldKs= newKs
        newKs= getKcentroids(Ss, oldKs)
        print(newKs)
        oldvalue= foo(oldSs, oldKs)
        newvalue= foo(Ss, newKs)
        if abs(oldvalue-newvalue)<0.5:
            print(f"equilibrium: {i+1}")
            break

if __name__=="__main__":
    main()
