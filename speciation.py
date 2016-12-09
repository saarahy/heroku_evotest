from deap import base, creator, gp, tools
from measure_tree import *
import numpy as np
#Inicializa las poblacion con una sola especie
def init_species(population):
    for ind in population:
        ind.specie(1)

#cuenta el numero de especies en total
#de toda la poblacion
def count_species(population):
    specie=list()
    #specie.append(0)
    for ind in population:
        if ind.get_specie()!= None and ind.get_specie() not in specie:
            specie.append(ind.get_specie())
    if not specie:
        return 0
    else:
        return len(specie)

#funcion para contar los individuos de una especie
def ind_specie(population):
    specie2=list()
    num=list()
    for ind in population:
        specie2.append(ind.get_specie())
    specie=sorted(specie2)
    if specie.count(specie[0])==len(population):
        num.append([specie[0],specie.count(specie[0])])
    else:
        for i in range(len(specie)):
            if specie[i]!=specie[i-1]:
                num.append([specie[i],specie.count(specie[i])])
    for ind in population:
        set_numind(ind, num)
    return num

#funcion para regresar grupos de individuos por especie
def specie_gpo(population):
    specie2=list()
    gpos=list()
    orderbyfit=list()
    orderbyfit=sorted(population, key=lambda ind:ind.get_specie())
    for i in range(1,len(orderbyfit)):
        if orderbyfit[i-1].get_specie()==orderbyfit[i].get_specie():
            specie2.append(orderbyfit[i-1])
            if i==len(orderbyfit)-1:
                specie2.append(orderbyfit[i])
                gpos.append([specie2])
        else:
            specie2.append(orderbyfit[i-1])
            gpos.append([specie2])
            specie2=list()
            if i==len(orderbyfit)-1:
                specie2.append(orderbyfit[i])
                gpos.append([specie2])
    return gpos

#regresa el numero de individuos en una especie dada
def get_specie_ind(individuo, population):
    cont=0
    specie=individuo.get_specie()
    for ind in population:
        if ind.get_specie()==specie:
            cont+=1
    return cont


#regresa el grupo de individuos en una especie dada
def getInd_perSpecie(specie, population):
    individuals=[]
    for ind in population:
        if ind.get_specie()==specie:
            individuals.append(ind)
    return individuals


#regresa el grupo de individuos en una especie de un individuo dado
def getInd_perIndividual(individuo, population):
    #cont=0
    individuals=[]
    specie=individuo.get_specie()
    for ind in population:
        if ind.get_specie()==specie:
            #cont+=1
            individuals.append(ind)
    return individuals

#funcion para contar los individuos de una especie
def list_species(population):
    specie2 = list()
    for ind in population:
        specie2.append(ind.get_specie())
    specie_list = np.unique(specie2)
    return specie_list

#funcion para asignar el numero de individuos
#en la especie, al individuo de esa misma especie

def set_numind(ind,species):
    for i in range(len(species)):
        if species[i][0]==ind.get_specie() and ind.num_specie!=None:
            ind.num_specie(species[i][1])

#revisa si algun individuo de la poblacion
#se encuentra sin especie y le asigna una
def species(population, h):
    num_specie=count_species(population)
    for ind in population:
        if ind.get_specie()==None:
            if (len(ind)==1):
                ind.specie(1)
            else:
                for ind1 in population:
                    if ind1.get_specie()!=None:
                        if distance(ind,ind1)<=h:
                            ind.specie(ind1.get_specie())
                            break
                if ind.get_specie()==None:
                    ind.specie(num_specie+1)
                    num_specie+=1
    return population

#asignar la especie de un individuo especifico
def specie_ind(population,ind, h):
    if (len(ind)==1):
        ind.specie(1)
    else:
        for ind_p in population:
            if ind_p.get_specie()!=None:
                if distance(ind, ind_p)<=h:
                    ind.specie(ind_p.get_specie())
                    break
        if ind.get_specie()==None:
            num_specie=count_species(population)
            ind.specie(num_specie+1)
    return ind

#asignar especies de padres a hijos
def specie_parents_child(parents, offspring, h):
    n_esp=count_species(parents)
    for ind in offspring:
        if ind.get_specie()==None:
            if (len(ind)==1):
                ind.specie(1)
            else:
                for parent in parents:
                    if parent.get_specie()!=None:
                        if distance(ind, parent)<=h:
                            ind.specie(parent.get_specie())
                            break
                if ind.get_specie()==None:
                    ind.specie(n_esp+1)
                    n_esp+=1
                    #parents.append(ind)
                    #offspring.remove(ind)
    return offspring#parents



