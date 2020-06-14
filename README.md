# lit_reviewer

![](https://png.pngtree.com/thumb_back/fw800/back_our/20190620/ourmid/pngtree-library-cartoon-banner-illustration-image_155272.jpg)

[![Generic badge](https://img.shields.io/badge/Python-3.8-<red>.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/License-MIT-blue.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/Maintained-Yes-green.svg)](https://shields.io/)
---

## Introduction

Ever wanted to easily build a flat-file for literature reviews from Scopus, Web of Science, and PubMed? Well, now you can!

## Inputs

You will need to change: 
  1. your email address!
  2. your scopus API key
  3. the list of queries you want to search for (example provided in query_list)
  
## Whats this stuff about flags?

* Check whether a paper mentions a specific search term: useful if combining different boolean terms!
* Easily check whether the paper features in a specific API return (we drop duplicates based on DOI\title)

## Pre-reqs

Simple! Just `pip install -r requirements.txt`! The web of science functionality relies heavily on the [wos](https://pypi.org/project/wos/) library

## Authorisation

You need an API key for scopus, and institutional authorisation. The WOS client is set up herein to be authorised based on network. Pubmed requires nothing (some functions there borrowed from [this](https://www.nature.com/articles/s42003-018-0261-x) paper).

## @TODO

* better error handling
* make classes
* unit testing
* a couple of fields are missing from the WOS parse
* please raise issues if you need something specific
* this is very, very much a prototy\work in progress!
