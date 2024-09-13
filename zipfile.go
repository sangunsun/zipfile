/*
Copyright 2024 sangunsun All Rights Reserved
本程序实现磁盘压缩、文件夹数据压缩、数据去重功能
实现方式是把给定目标中重复的文件进行数据去重，
重复文件只保留一份文件数据,但不会删除任何一个文件，所有文件共享一份数据
本软件可独立运行，不需要安装sqlite数据库
*/
package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"time"

	//"time"
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

var db *sql.DB 
var dataBf bytes.Buffer 
var goos string

var beginPath string
var delay int
var minSize int64
var isNewScan string //为了保证磁盘上的文件信息和数据库中文件信息一致性，不造成误删除，取消该参数
var isOnlyScan string
var delMode string
var repeatFile string
var ignore string
var filter string
var isSilent string

var pbeginPath = flag.String("path","","需要扫描处理的目录路径")
var pdelay = flag.Int("delay",0,"文件的最后修改时间离处理时的最短时间间隔,单位:秒")
var pminSize = flag.Int64("minsize",1000,"最小文件大小，单位字节")
var pisNewScan= flag.String("newscan","yes","yes/no,是否进行新的扫描")
var pisOnlyScan = flag.String("onlyscan","yes","yes/no,是否只扫描不执行数据删除操作")
var pdelMode = flag.String("delmode","data","data/file,删除模式，删除文件或只删除数据")
var prepeatFile = flag.String("rfile","repeatFile.csv","把重复文件列表保存到哪个文件")
var pignore = flag.String("ignore",`^/proc|^/dev|^/sys|^/run|^/swp`,"以正则表达式表示的需要忽略的文件清单")
var pfilter = flag.String("filter",".*","只包含哪些文件--以正则表达式表示")
var pisSilent = flag.String("s","no","yes/no,是否开启静默模式，不需要手动确认输入参数")

const layoutDB = "2006-01-02T15:04:05Z"
const layoutF = "2006-01-02 15:04:05"

func init(){

    goos= runtime.GOOS

    flag.Parse()
    beginPath = *pbeginPath
    delay = *pdelay
    minSize = *pminSize
    isNewScan = *pisNewScan
    isOnlyScan = *pisOnlyScan
    delMode = *pdelMode
    repeatFile = *prepeatFile
    ignore = "(?m)" + *pignore
    filter = "(?m)" + *pfilter
    isSilent = *pisSilent

    var err error
    //db, err = sql.Open("sqlite", ":memory:")
    db, err = sql.Open("sqlite", "./file.db")

    if err != nil {
        panic("connect db error" + err.Error())
    }
}

func main() {
    defer db.Close()

    //输入参数检查
    if isNewScan != "yes" && isNewScan !="no" || isOnlyScan != "yes" && isOnlyScan != "no"||delMode !="file" && delMode != "data"{
        log.Println("输入参数值错误！")
        return 
    }
    fmt.Printf("-newscan %s,-onlyscan %s,-delmode %s\n",isNewScan,isOnlyScan,delMode)

    if isSilent !="yes" {
        reader := bufio.NewReader(os.Stdin)
        fmt.Print("请仔细查看以上参数，确认是否继续？yes/no: ")
        input, _ := reader.ReadString('\n')
        input = strings.TrimSpace(input)

        if input != "yes" {
            return
        }

    }

    if isNewScan == "yes" {
        if _,err := db.Exec(`drop table if exists tfile`);err != nil{
            panic(" delete table error:"+ err.Error())
        }

        if _,err := db.Exec(`create table tfile(name varchar(300),
        abspath varchar(2000),
        type int,
        size long,
        linkcount int,
        md5  varchar(32),
        mtime datetime);`); err != nil{
            panic("create table error:" + err.Error())
        }
        log.Println("开始扫描目录:",beginPath)
        //统计目标目录里所有的文件信息
        treeFile(beginPath) 
    }

    //把所有重复文件分组输出到文件中
    fmt.Println("正在向文件:",repeatFile,"写入重复文件信息...")
    showRepeatFile(repeatFile)

    //执行删除重复文件操作
    if isOnlyScan == "yes" {
        fmt.Println("正在进行文件重复数据信息统计...")
    }else {
        fmt.Println("正在清理文件...")
    }
    zipdata()

}

func treeFile(rootPath string) error{
    //filepath.Walk(rootPath,walkback)

    if _,err :=os.Stat(rootPath);err != nil{
        log.Println("给定目录错误：",rootPath,err)
        return err
    }

    dataBf.Reset()

    sqlStr := "insert into tfile values "
    dataBf.WriteString(sqlStr)

    timeBegin:=time.Now()
    err :=filepath.WalkDir(rootPath,walkdirback3)
    if err != nil {
        log.Println("遍历目录出错",err)
        return err
    }
    timeEnd:= time.Now()

    fmt.Printf("begin time:%v ,end time %v ,cost time:%v\n",timeBegin, timeEnd,timeEnd.Sub(timeBegin))
    dataByte:=dataBf.Bytes()

    if len(dataByte)==len(sqlStr){
        return nil
    }
    dataByte[len(dataByte)-1]=';'

    _,err =db.Exec(string(dataByte))
    if err!=nil{
        log.Println("插入数据失败",err)
    }

    //创建两个关键字段索引
    sqlStr = "create index idx_md5 on tfile (md5);"
    _, err = db.Exec(sqlStr)
    if err != nil{
        log.Println("创建md5索引失败",err)
    }

    sqlStr = "create index idx_linkcount on tfile (linkcount);"
    _, err = db.Exec(sqlStr)
    if err != nil{
        log.Println("创建linkcount索引失败",err)
    }
    return nil
}

//利用SQL语句特点批量插入数据，每次启动程序都全新扫描文件系统，不再保存旧文件信息。
func walkdirback3(path string, d fs.DirEntry, err error) error {
    info,_:=d.Info()
    absPath,_:=filepath.Abs(path)

    if info == nil {
        return nil
    }

    reignore := regexp.MustCompile(ignore)
    reinclude := regexp.MustCompile(filter)
    //只对普通文件进行处理
    if d.Type() & os.ModeType !=0 || 
    
    //去除需要忽略的文件
    reignore.MatchString(absPath) ||

    //只包含include清单中的文件
    !reinclude.MatchString(absPath) ||

    //去除小文件，默认去除大小1000字节以下文件
    info.Size()< minSize ||

    //如果文件的最后修改时间离现在不到一定时间可能是日志文件也不处理
    info.ModTime().Add(time.Second*time.Duration(delay)).After(time.Now()) {
        return nil
    }

    //如果计算文件md5码失败，则不再处理该文件
    md5Str := md5File(absPath)
    if md5Str == ""{
        return nil
    }

    //没有linkcount，其实也是可以进行数据删重处理的，只不过会做一些无用的删重操作
    //这里还是采用计算linkcount失败，就不再处理该文件
    linkCount := getHardLinks(absPath)
    if linkCount == 0{
        return nil
    }

    //如果数据库中没有保存过该文件则插入该文件-----------
    dataBf.WriteString( fmt.Sprintf("('%s','%s','%d',%d,%d,'%s','%s'),",
    strings.ReplaceAll(info.Name(),"'",`''`),
    strings.ReplaceAll(absPath,"'",`''`),
    d.Type(),
    info.Size(),
    linkCount,
    md5Str,
    info.ModTime().Format("2006-01-02 15:04:05")))

    fmt.Println(absPath,",",info.Size(),",",linkCount,",",md5Str)

    return nil
}

func md5File(fileName string) string {

    // 打开文件
    file, err := os.Open(fileName)

    if err != nil {
        log.Println("open file err:",err)
        return ""
    }
    defer file.Close()

    //只对普通文件进行md5处理
    fileInfo,err := file.Stat()
    if fileInfo.Mode().Type() & os.ModeType !=0 {
        log.Println("file is not a normal files:",err,fileInfo.Mode().Type() )
        return ""
    }

    // 创建md5哈希实例
    hash := md5.New()
    // 从文件中读取数据并写入到哈希实例中
    if _, err := io.Copy(hash, file); err != nil {
        log.Println("copy file err:",err,"file type is:",fileInfo.Mode().Type() )
        return ""
    }
    // 计算最终的哈希值
    hashBytes := hash.Sum(nil)

    return  fmt.Sprintf("%x", hashBytes) 
}

func removeFile(filePath string) error {
    if err:=os.Remove(filePath);err!= nil {
        return errors.New("删除文件出错:"+filePath + err.Error())
    }
    return nil
}

func removeAndHardLinkFile(sourcePath string,linkPath string) error {
    if err:=os.Remove(linkPath);err!= nil {
        return errors.New("删除文件出错:"+linkPath + err.Error())
    }
    //创建硬链接，如果创建硬链接失败则尝试创建软链接
    if err := hardLinkFile(sourcePath,linkPath); err != nil {
        if err := os.Symlink(sourcePath,linkPath); err != nil {
            return errors.New("创建硬软链接都出错:" + err.Error())
        }
    }
    return nil
}

//判断是否要对两个文件进行数据冗余消除操作，取决于如下几个参数
//1.两个文件是否有相同的md5值
//2.两个文件是否本来就已经消除数据冗余了---inode引用数大于1
//即各文件inode中的最大引用数是否小于md5值相同的文件数
func zipdata() error{
    sqlStr:=`select md5 from
    (SELECT
    tb1.*,
    (SELECT COUNT(*) FROM tfile AS tb2 WHERE tb1.md5 = tb2.md5) AS md5_count
    FROM
    tfile AS tb1 ) as tt
    where linkcount < md5_count
    group by md5;`

    //从数据库找出所有md5出现次数大于文件linkcount的md5码
    rows,err := db.Query(sqlStr)
    if err!=nil{
        return err
    }

    //把找出的md5码载入一个字符串切片中
    var md5Strs []string
    for rows.Next() {
        var md5Str string
        err := rows.Scan(&md5Str)
        if err != nil {
            rows.Close()
            log.Println(err)
            return err
        }
        // 将读取的值添加到切片中
        md5Strs = append(md5Strs, md5Str)
    }
    rows.Close()

    //遍历md5字符串切片，并从数据库中找出对应的文件
    //对找出的文件进行数据压缩操作,进行文件压缩的同时也从数据库中删除相应的记录
    //把时间最老的文件做基准文件，其它文件和该文件做硬链接
    //该函数同时统计节省的空间大小,统计的时候单位为字节，之后按1000进制换算成M
    //是否真正删除了数据是无法从文件信息中统计出来的，
    //只能在执行删除之前根据inode的引用数是否为1，从而判断是否真正删除了数据
    var zipSize int64 //节省的磁盘空间
    var fileSize int64  //每个文件的大小
    var mtime string  //文件最后修改时间
    repeatFileTotal :=0  //重复文件个数
    repeatGroupTotal := len(md5Strs) //有多少组重复文件
    repeatGroupNum :=0
    zipSize =int64(0) 

    var oldMd5 =""
    var md5Str =""
    var sourcePath =""
    var dPath =""
    sqlStr =`select abspath,size,mtime,md5 from
    (SELECT tb1.*,(SELECT COUNT(*) FROM tfile AS tb2 WHERE tb1.md5 = tb2.md5) AS md5_count
    FROM
    tfile AS tb1 ) as tt
    where linkcount < md5_count
    order by md5,mtime;
    `
    stmt,err := db.Prepare(sqlStr)
    if err != nil {
        return err
    }
    defer stmt.Close()

    rows,err = stmt.Query(sqlStr) 
    if err != nil {
        return err
    }

    /*
    用于统计各函数消耗时间的临时变量，正式版去除相关代码 
    */
    var timeGetHardLink time.Duration
    var timeGetFileInfo time.Duration
    var timeParseTime time.Duration
    var timeNext time.Duration
    
    var beginTime time.Time
    var endTime time.Time

    beginTime = time.Now()
    for rows.Next() {
        endTime =time.Now() 
        timeNext = timeNext + endTime.Sub(beginTime)
        
        err := rows.Scan(&dPath,&fileSize,&mtime,&md5Str)
        if err != nil {
            rows.Close()
            return err
        }

        //如果读文件信息失败，则证明该文件有问题跳过处理
        beginTime = time.Now()
        fileInfo,err := os.Stat(dPath)
        endTime = time.Now()
        timeGetFileInfo = timeGetFileInfo + endTime.Sub(beginTime)

        if err !=nil {
            continue
        }
        //如果文件最后修改时间和数据库中不一样，证明文件已被修改过
        //放弃处理该文件
        beginTime =time.Now()
        parseMtimeDB,err := time.Parse(layoutDB,mtime)
        parseMtimeF ,_ := time.Parse(layoutF,fileInfo.ModTime().Format("2006-01-02 15:04:05"))
        endTime = time.Now()

        timeParseTime = timeParseTime + endTime.Sub(beginTime)
        if parseMtimeDB != parseMtimeF{
            continue
        }
        repeatFileTotal = repeatFileTotal + 1 //找到一个文件则总文件数加1

        //如果md5码切变，证明开始了新的一组文件
        if md5Str != oldMd5 {
            oldMd5 = md5Str
            sourcePath =dPath
            repeatGroupNum =repeatGroupNum +1
            fmt.Printf("\r共有%d组文件需要统计或处理，目前处理第%d组",repeatGroupTotal,repeatGroupNum)
        }else {
            //计算节省的磁盘空间是否真正删除了数据是无法从文件信息中统计出来的，
            //只能在执行删除之前根据inode的引用数是否为1，从而判断是否真正删除了数据
            linkCount :=0
            beginTime =time.Now()
            linkCount = getHardLinks(dPath)
            endTime = time.Now()
            timeGetHardLink = timeGetHardLink + endTime.Sub(beginTime)

            if linkCount == 1{
                zipSize = zipSize + int64(fileSize) 
            }

            if isOnlyScan == "no" {
                if delMode == "data" {
                    err=removeAndHardLinkFile(sourcePath,dPath)
                    if err !=nil{
                        log.Println("压缩文件失败",sourcePath,dPath,err)
                    }
                }else if delMode == "file"{
                    err = removeFile(dPath)
                    if err !=nil{
                        log.Println("删除文件失败",sourcePath,dPath,err)
                    }
                }
            }

        }
        beginTime = time.Now()
    }
    rows.Close()

    fmt.Printf("\n Cost time,rows.Next:%v,getHardLink:%v,getFileInfo:%v,parseTime:%v\n",timeNext,timeGetHardLink,timeGetFileInfo,timeParseTime)
    if isOnlyScan == "yes" {
        fmt.Println("数据重复文件个数为:",repeatFileTotal,
        ",共:",repeatGroupTotal,
        "组重复文件，节省:",float32(zipSize)/1000000,"M磁盘空间(估算数据仅供参考,要删除数据请添加参数 -onlyscan no 参数)")
    }else if isOnlyScan == "no"{

        fmt.Println("数据重复文件个数为:",repeatFileTotal,
        ",共:",repeatGroupTotal,"组重复文件，节省:",float32(zipSize)/1000000,"M磁盘空间")
    }
    return nil
}

func showRepeatFile(fileName string) error {
    sqlStr:=`SELECT
    tb1.md5,abspath,size,mtime,(SELECT COUNT(*) FROM tfile AS tb2 WHERE tb1.md5 = tb2.md5) AS md5_count
    FROM
    tfile AS tb1 
    where  md5_count>1 order by size desc,md5,mtime desc;`

    var md5,absPath,mtime string
    var size,md5Count int

    rows,err := db.Query(sqlStr)
    if err!=nil{
        return err
    }
    // 打开文件
    file, err := os.OpenFile(fileName,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
    if err != nil {
        log.Println("open file err:",err)
        return err
    }
    defer file.Close()

        _,err =fmt.Fprintf(file, "%s\n",  time.Now().Format(layoutF)) 
        _,err =fmt.Fprintf(file, "%s, %s, %s, %s\n",  "文件绝对路径", "文件大小", "文件最后修改时间","文件md5") 

    for rows.Next(){
        rows.Scan(&md5,&absPath,&size,&mtime,&md5Count)
        _,err :=fmt.Fprintf(file, "%s, %d, %s, %s\n", absPath, size, mtime, md5) 
        if err!= nil {
            return err
        }

        if err =rows.Err();err !=nil{
            return err
        }
    }

    return nil
}
