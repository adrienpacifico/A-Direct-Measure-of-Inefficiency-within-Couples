# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 08:48:27 2017

@author: IMPTEMP_A_PACIFIC
This is for 2015 income
"""
from __future__ import division
import numpy as np
from numpy import (datetime64, logical_and as and_, logical_not as not_, logical_or as or_, logical_xor as xor_,
    maximum as max_, minimum as min_, round)
# import pandas as pd #pd.set_option("display.max_columns", 200)


rates = np.array([0, 0.14, 0.3, 0.41, 0.45]) #To modify for specific year 2014
thresholds = [0, 9690, 26764, 71754, 151956]  #To modify for specific year 2014


# permet de calculer l'impot à partir d'une base taxable
# de seuils et de taux
def calc(base=None, thresholds=None, rates=None):
    base1 = np.tile(base, (len(thresholds), 1)).T
    thresholds1 = np.tile(np.hstack((thresholds, np.inf)), (len(base), 1))
    a = np.maximum(np.minimum(base1, thresholds1[:, 1:]) - thresholds1[:, :-1], 0)
    return np.sum(a * rates, axis=1)
    # return np.dot(self.amounts, a.T > 0)


def parts_fiscales_enfants(nb_enfants=None):
    parts_fiscales_enfants = (
        0.5 * (nb_enfants == 1) +
        1 * (nb_enfants == 2) +
        (nb_enfants - 1) * (nb_enfants > 2)
            )
    return parts_fiscales_enfants


def ir_sans_qf(rni=None, thresholds=thresholds, rates=rates):
    '''
    Impôt sans quotient familial
    '''
    rni = rni
    return calc(rni, thresholds, rates)


def ir_avec_qf(rni=None, parts_fiscales_enfant=None, thresholds=thresholds, rates=rates):
    '''
    Impôt avec quotient familial
    '''
    rni = rni
    parts_fiscales = parts_fiscales_enfant + 1.0

    return calc(rni/(parts_fiscales), thresholds, rates) * parts_fiscales


def ir_avec_plafond_qf_enfant(rni=None, parts_fiscales_enfant = None, thresholds= thresholds, rates = rates):
    ac_qf = ir_avec_qf(rni=rni, parts_fiscales_enfant=parts_fiscales_enfant)
    ss_qf = ir_sans_qf(rni=rni)
    qf_threshold = (parts_fiscales_enfant) * 1510*2  #To modify for specific year 2014
    ir_plaf_qf = (
        (
            (( ss_qf - ac_qf ) > qf_threshold) *  # si gain supérieur à threshold
                (ss_qf-qf_threshold)  # on paye sans qf avec threshold
        ) +       
        (
         (((ss_qf - ac_qf) <= qf_threshold) * ac_qf)
        )
    )
    return ir_plaf_qf



class decote_param:
    seuil_celib = 1135

def decote_ir(rni=None, parts_fiscales_enfant = None):

    ir_plaf_qf = ir_avec_plafond_qf_enfant(rni=rni, parts_fiscales_enfant = parts_fiscales_enfant)
#    nb_adult = 1
    nb_adult = 1
    decote_seuil_celib = decote_param.seuil_celib
    #decote_seuil_couple = simulation.legislation_at(period.start).ir.decote.seuil_couple
    decote_celib = ((ir_plaf_qf < decote_seuil_celib) * 
        (decote_seuil_celib - ir_plaf_qf))  #To modify for specific year 2014
    #decote_couple = (ir_plaf_qf < decote_seuil_couple) * (decote_seuil_couple - ir_plaf_qf)
    
    return (nb_adult == 1) * decote_celib #+ (nb_adult == 2) * decote_couple

#    ir_plaf_qf = ir_avec_plafond_qf_enfant(rni=rni, parts_fiscales_enfant = parts_fiscales_enfant)
#    nb_adult = 1
#    decote_seuil_celib = decote.seuil_celib
#    decote_celib = (ir_plaf_qf < 4 / 3 * decote_seuil_celib) * (decote_seuil_celib - 3 / 4 * ir_plaf_qf)
#
#    return decote_celib 




########
########
#Transformation revenus######
########
########



class abatpro:
    max = 12157 #To modify for specific year 2014
    min = 426 #To modify for specific year 2014
    taux = .1 #To modify for specific year 2014


def salcho_imp(rev_sal = None):
    # On ne prend plus en compte le fait d'eêtre chomeur de longue durée.

    rev_sal = rev_sal #somme de salaire et chomage imposable dans le code OF
    frais_reels = np.zeros(len(rev_sal))
    abattement_minimum = abatpro.min  # * not_(chomeur_longue_duree) + abatpro.min2 * chomeur_longue_duree
    abatfor = round(min_(max_(abatpro.taux * rev_sal, abattement_minimum), abatpro.max))
    return (
        (frais_reels > abatfor) * (rev_sal - frais_reels) + 
        (frais_reels <= abatfor) * max_(0, rev_sal - abatfor)
        ) 




class abatpen:
    min = 379 #To modify for specific year 2014
    max = 3707 #To modify for specific year 2014
    taux = 0.10 #To modify for specific year 2014


def pen_net(rev_pen_var=None):
    rev_pen_var = rev_pen_var
    return max_(0,
                rev_pen_var - round(
                    max_(abatpen.taux * rev_pen_var, abatpen.min)))







    
ppe_seuils = [3743,12475, 17451, 24950, 26572 ]
ppe_taux = [0.077,0.193, 0.051]

def ppe(rni, nombre_enfants ):

    ppe_calc = 0.077    
    ppe = (0+
        (((rni>3743) & (rni<12475))*rni * 0.077)
        +
        (((rni>12475) & (rni<17451)) * ((17451 - rni)) * .193))
    marjoration_enfants =  (36 * nombre_enfants) * ((rni <17451)& (rni>0))
        
    return ppe + marjoration_enfants
    
    