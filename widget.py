# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 15:45:48 2019

@author: klosmarc
"""

import ipywidgets as widgets
from IPython.display import display

w=widgets.Dropdown(
    options=['1', '2', '3'],
    value='2',
    description='Number:',
    disabled=False,
)

display(w)