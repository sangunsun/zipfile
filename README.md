# zipfile
Eliminate redundant files to save disk space，一个用于删除重复文件的冗余数据，节省磁盘空间的小工具，使用后既节省了磁盘空间也不会改变原文件结构

本软件对指定目录进行文件查重，如果发现两个文件内容完全相同且有多份数据，则删除多余的数据，只保留一份数据，其它地方文件以硬链接方式指向文件唯一的数据区

#### 不支持windows，支持linux和mac os

#### 程序的输入参数
1. -path:扫描哪个目录，默认：./
2. -onlyscan:是否只扫描不执行数据删除，取值范围："yes/no,默认："yes")
3. -delmode:删除模式，删除文件或只删除数据：取值范围：data/file，默认:data(删除冗余数据)
4. -minsize:多少大小以下的文件忽略不扫描，单位字节，默认值：1000字节
5. -delay:文件最后修改时间离扫描时的最小时间间隔(避免扫描频繁更新的日志文件)，单位：秒，默认值：0

#### 输出
1. 扫描到的各文件名及md5码，名为file.db的sqlite3数据库中保存有：`文件名，绝对文件名，文件大小，文件数据引用数，文件最后修改时间，文件的md5码`

2. 文件统计数据  
+ 如果是只扫描  
`数据重复文件个数为: 0 ,共: 0 组重复文件，节省: 0 M磁盘空间(估算数据仅供参考)`  

+ 如果是扫描并删除冗余数据  
`数据重复文件个数为: 0 ,共: 0 组重复文件，节省了: 0 M磁盘空间`

#### 使用示例
+ 对当前目录进行扫描查看有多少重复文件，不删除数据  
` zipfile `

+ 只查看一下`/home`目录中有多少重复的文件，不删除冗余文件或数据  
`zipfile -path /home`

+ 把`/home`目录中重复文件的冗余数据删除，但不影响原文件目录结构(看起来文件一个都没少)  
`zipfile -path /home -onlyscan no`

+ 把`/home`目录中的重复文件删除，每组重复文件只保留一个时间最早的文件    
`zipfile -path /home -onlyscan no -delmode file`
