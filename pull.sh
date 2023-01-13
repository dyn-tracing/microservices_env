#!/bin/bash  
  
url='http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData'



for((i=1;i<=2;i++));  
do   
	command="curl ${url}/MSCallGraph/MSCallGraph_${i}.tar.gz -o MSCallGraph_${i}.tar.gz"
	${command}
done 

