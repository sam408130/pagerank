#!/usr/bin/python
#-*-coding:utf-8-*-

import re
import math
import time
import os
import anydbm
def buildMat():
	#data = open(filepath,'r')
	db = anydbm.open('.\\data\\yanchang.db','r')
	indexMat = {}
	indexMat_R = {}
	count = -1
	print len(db)
	for i in db:

		#i = i.strip('\n')
		if not indexMat.has_key(i):count += 1
		indexMat[i] = count
		indexMat_R[count] = i
		
	db.close()
	return indexMat,indexMat_R

def buildTransitionMatrix(indexMat):
	def voteConnect(index1,index2):

		if VoteCon.has_key(index1):
			tempmat = VoteCon[index1]
			
			tempmat[index2] = ''
			VoteCon[index1] = tempmat
			#if not re.search(index2,tempmat):
			#	VoteCon[index1] += ','+index2
		else:
			tempmat = {}
			tempmat[index2] = ''
			VoteCon[index1] = tempmat
		
		if VoteCon.has_key(index2):
			tempmat = VoteCon[index2]
			tempmat[index1] = ''
			VoteCon[index2] = tempmat
		else:
			tempmat = {}
			tempmat[index1]
			VoteCon[index2] = tempmat
		
	def processfile(filename,th):
		db = anydbm.open(filename,'r')
		count = 0
		templen = len(db)
		for i in db:                                                                                                    
			count  += 1
			
			mat = re.compile(',').split(db[i])
			print count , len(mat),templen
			if len(mat) > th:continue
			#print len(mat)
			for i in range(0,len(mat)-1):
				try:
					index_a = indexMat[mat[i]]
					index_b = indexMat[mat[i+1]]
					voteConnect(index_a,index_b)
				except:
					continue


			'''
			for a in mat:
				if indexMat.has_key(a):
					index_a = indexMat[a]
					l = mat.index(a)	
					while(l<len(mat)):
						l += 1
						b = mat[l-1]
						if indexMat.has_key(b):
							index_b = indexMat[b]
							voteConnect(str(index_a),str(index_b))						
			'''










					#for b in mat:
					#	if a == b:continue

		print filename,'	done!',time.strftime("%Y-%m-%d %A %X ", time.localtime()) 
		db.close()




	global VoteCon
	#global VoteCon_db
	#VoteCon_db = anydbm.open('VoteCon.db','n')
	VoteCon = {}
	processfile('.\\data\\artist2yc.db',8000)
	print len(VoteCon)
	processfile('.\\data\\album2yc.db',100000)
	print len(VoteCon)
	#processfile('.\\data\\song2yc.db',1000000)
	print len(VoteCon)
	processfile('.\\data\\songlist.db',100)
	print len(VoteCon)
	'''
	for i in VoteCon_db:
		temphash = {}
		mat = re.compile(',').split(VoteCon_db[i])
		for j in mat:
			temphash[int(j)] = ''
		VoteCon[int(i)] = temphash
	'''
	
	return VoteCon

def buildOutDegree(length,VoteCon):
	outDegree={}
	loop=0
	mat=[]
	for i in VoteCon:
		loop+=1
		mat=VoteCon[i].keys()
		for j in mat:

			if outDegree.has_key(j):
				outDegree[j] += 1
			else:
				outDegree[j] = 1
	NoneCon = {} 
	mat1=[]
	for i in range(0,length):
		if not outDegree.has_key(i):
			NoneCon[i] = 1
	return outDegree,NoneCon

def pagerank(length,threshold,VoteCon,outDegree,NoneCon,indexMat_R,outfile_path):
	def calNewPR(PR,length):
		p=0.85
		error = 0
		new_PR={}
		loop1=0
		keys=PR.keys()
		temp=0.0
		for each in NoneCon.keys():
			temp+=float(PR[each])
		calC=temp/float(length)		
		for i in keys:
			loop1+=1
			calB=0.0
			if VoteCon.has_key(i):
				mat_votei=VoteCon[i].keys()
				for each in mat_votei:
					calB+=float(float(PR[each])/float(outDegree[each]))		
			new_PR[i]=p*(calB+calC)+(1-p)
			error += abs(new_PR[i] - PR[i])
		return new_PR,error

	def decision(error,threshold):
		print 'iteration:'+str(loop)+'  error:'+str(error)+'  time:%s'%time.strftime("%Y-%m-%d %A %X ", time.localtime())  
		if error<threshold:
			return 'true'
		else:
			return 'fault'


	def iteration(PR,threshold,length):
		global loop
		loop=0	
		while(1):
			loop+=1
			new_PR,error=calNewPR(PR,length)
			


			mybool=decision(error,threshold)
			if mybool=='true':
				return new_PR
				break
			else:
				PR=new_PR



	PR={}
	for i in range(0,length):
		PR[i] = 1

	finalPR = iteration(PR,threshold,length)
	out0 = open(outfile_path,'w')
	out = anydbm.open('.\\data\\uriToPR.db','n')
	for i in finalPR:
		try:
			out0.write(indexMat_R[i]+'##'+str(finalPR[i])+'\n')
			out[str(indexMat_R[i])] = str(finalPR[i])
		except:
			print 'error index:',i


def initialHot(hot_file,indexMat):
	db = anydbm.open(hot_file,'r')
	print 'hot len:',len(db)
	hothash = {}
	for i in indexMat:
		try:
			h = float(db[i])
		except:
			h = float(1)
		hothash[indexMat[i]] = h
	maxhot = max(hothash.values())
	hot = {}
	for i in hothash:
		hot[i] = hothash[i]/float(maxhot)
	return hot,maxhot


def main_process():
	print time.strftime("%Y-%m-%d %A %X ", time.localtime()) 

	#indexMat,indexMat_R = buildMat()	

	threshold = 0.01

	#idfile_path   所有实体id的文件夹路径，id必须是唯一的
	indexMat,indexMat_R = buildMat()
	print 'indexMat len:',len(indexMat)
	print 'build indexMat done!'
	length = len(indexMat)	#file_link	盛放album2song,artist2album,album2song所有关系的文件夹路径
	#hot,maxhot = initialHot(hot_file,indexMat)
	#print 'initial hot done ,maxhot:',maxhot
	VoteCon = buildTransitionMatrix(indexMat)	
	print 'votecon len:',len(VoteCon)
	print 'build TransitionMatrix done!'
	outDegree,NoneCon = buildOutDegree(length,VoteCon)
	print 'outDegree,NoneCon lens:',len(outDegree),len(NoneCon)
	print 'build outDegree done!'
	pagerank(length,threshold,VoteCon,outDegree,NoneCon,indexMat_R,'prvalue2.txt')
	


def preprocess():
	os.mkdir('data')
	print 'pre-processing~~~~'
	uri2name = anydbm.open('.\\data\\uri2name.db','c')
	yc = anydbm.open('.\\data\\yanchang.db','c')
	song2yc = anydbm.open('.\\data\\song2yc.db','c')
	album2yc = anydbm.open('.\\data\\album2yc.db','c')
	artist2yc = anydbm.open('.\\data\\artist2yc.db','c')
	sl = anydbm.open('.\\data\\songlist.db','c')
	name2id = anydbm.open('.\\data\\name2id.db','c')
	hot = anydbm.open('.\\data\\uriToHot.db','c')
	hothash = {}

	reg_sl = '歌单'#.decode('utf-8').encode('gbk')
	reg_al = '专辑'#.decode('utf-8').encode('gbk')
	def parsefile(filename):
		data = open(filename,'r')
		for i in data:
			mat = re.compile('	').split(i.strip('\n'))
			
			if mat[1] == 'procName':
				if re.search(reg_sl,mat[0]):continue
				#uri2name[mat[0]] = mat[2]
				if uri2name.has_key(mat[0]):
					uri2name[mat[0]] += '/'+mat[2]
				else:
					uri2name[mat[0]] = mat[2]
				if name2id.has_key(mat[2]):
					tempstr = name2id[mat[2]]
					tempstr += ','+mat[0]
					name2id[mat[2]] = tempstr
				else:
					tempstr = mat[0]
					name2id[mat[2]] = tempstr
				continue			
		
			if mat[1] == 'songId':
				if re.search(reg_sl,mat[0]):
					if sl.has_key(mat[0]):
						tempstr = sl[mat[0]]
						tempstr += ','+mat[2]
						sl[mat[0]] = tempstr
					else:
						tempstr= mat[2]
						sl[mat[0]] = tempstr
			
				else:
					if yc.has_key(mat[0]):
						tempstr = yc[mat[0]]
						tempstr += '<song:%s>'%mat[2]
						yc[mat[0]] = tempstr
					else:
						yc[mat[0]] = '<song:%s>'%mat[2]
				
					if song2yc.has_key(mat[2]):
						tempstr = song2yc[mat[2]]
						tempstr += ','+mat[0]
						song2yc[mat[2]] = tempstr
					else:
						song2yc[mat[2]] = mat[0]
					continue
			
		
			if mat[1] == 'albumId':
				if re.search(reg_sl,mat[0]):continue
				if yc.has_key(mat[0]):
					tempstr = yc[mat[0]]
					tempstr += '<album:%s>'%mat[2]
					yc[mat[0]] = tempstr
				else:
					yc[mat[0]] = '<album:%s>'%mat[2]
				if album2yc.has_key(mat[2]):
					tempstr = album2yc[mat[2]]
					tempstr += ','+mat[0]
					album2yc[mat[2]] = tempstr
				else:
					album2yc[mat[2]] = mat[0]
				continue	
		
			if mat[1] == 'artistId':
				if re.search(reg_sl,mat[0]):continue
				if re.search(reg_al,mat[0]):continue			
				if yc.has_key(mat[0]):
					tempstr = yc[mat[0]]
					tempstr += '<artist:%s>'%mat[2]
					yc[mat[0]] = tempstr
				else:
					yc[mat[0]] = '<artist:%s>'%mat[2]
				if artist2yc.has_key(mat[2]):
					tempstr = artist2yc[mat[2]]
					tempstr += ','+mat[0]
					artist2yc[mat[2]] = tempstr
				else:
					artist2yc[mat[2]] = mat[0]
				continue
			
			if mat[1] == 'hot':
				if hothash.has_key(mat[0]):
					hotmat = hothash[mat[0]]
					if re.search('baidu',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[0] = str(tempmat[1])
					elif re.search('qq',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[1] = str(tempmat[1])
					elif re.search('wangyi',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[2] = str(tempmat[1])				
					hothash[mat[0]] = hotmat
				else:
					hotmat = ['nil','nil','nil']
					if re.search('baidu',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[0] = str(tempmat[1])
					elif re.search('qq',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[1] = str(tempmat[1])
					elif re.search('wangyi',mat[2]):
						tempmat = re.compile(r'\|').split(mat[2])
						hotmat[2] = str(tempmat[1])				
					hothash[mat[0]] = hotmat				
		
		print filename,'	done!'


	parsefile('triple0.txt')
	parsefile('triple1.txt')
	parsefile('triple2.txt')
	parsefile('triple3.txt')
	parsefile('triple4.txt')
	parsefile('triple5.txt')









if __name__=='__main__':
	#filestr = 'ids#links#songlist2song#UriToHot#prvalue_hot'
	preprocess()
	main_process()
	#buildMat('ids.txt')
	#db = anydbm.open()
