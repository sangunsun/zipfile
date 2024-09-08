/*
Copyright 2024 sangunsun All Rights Reserved
本程序实现磁盘压缩、文件夹数据压缩、数据去重功能
实现方式是把给定目标中重复的文件进行数据去重，
重复文件只保留一份文件数据,但不会删除任何一个文件，所有文件共享一份数据
本软件使用sqlite3保存文件信息，但本软件可独立运行，不需要安装sqlite数据库
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
    "time"
    "syscall"
    "io"
    "io/fs"
    "os"
    "path/filepath"
    "strings"
    _ "modernc.org/sqlite"
)

var db *sql.DB 
var dataBf bytes.Buffer 

var beginPath string
var delay int
var minSize int64
//var isNewScan string //为了保证磁盘上的文件信息和数据库中文件信息一致性，不造成误删除，取消该参数
var isOnlyScan string
var delMode string
var repeatFile string

var pbeginPath = flag.String("path","./","需要扫描处理的目录路径")
var pdelay = flag.Int("d",0,"文件的最后修改时间离处理时的最短时间间隔")
var pminSize = flag.Int64("minsize",1000,"最小文件大小，单位字节")
//var pisNewScan= flag.String("newscan","yes","yes/no,是否进行新的扫描")
var pisOnlyScan = flag.String("onlyscan","yes","yes/no,是否只扫描不执行数据删除操作")
var pdelMode = flag.String("delmode","data","data/file,删除模式，删除文件或只删除数据")
var prepeatFile = flag.String("rfile","repeatFile.txt","有冗余数据的重复文件列表")

func init(){

    flag.Parse()
    beginPath = *pbeginPath
    delay = *pdelay
    minSize = *pminSize
    //   isNewScan = *pisNewScan
    isOnlyScan = *pisOnlyScan
    delMode = *pdelMode
    repeatFile = *prepeatFile

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
    if isOnlyScan != "yes" && isOnlyScan != "no"||delMode !="file" && delMode != "data"{
        log.Println("输入参数错误！")
        return 
    }


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

    log.Println(beginPath)
    //统计目标目录里所有的文件信息
    treeFile(beginPath) 
    //执行删除重复文件操作
    zipdata()

}

func treeFile(rootPath string){
    //filepath.Walk(rootPath,walkback)
    dataBf.Reset()

    strSql := "insert into tfile values "
    dataBf.WriteString(strSql)

    timeBegin:=time.Now()
    filepath.WalkDir(rootPath,walkdirback3)
    timeEnd:= time.Now()

    fmt.Printf("begin time:%v ,end time %v ,cost time:%v\n",timeBegin, timeEnd,timeEnd.Sub(timeBegin))
    dataByte:=dataBf.Bytes()

    if len(dataByte)==len(strSql){
        return
    }
    dataByte[len(dataByte)-1]=';'

    _,err:=db.Exec(string(dataByte))
    if err!=nil{
        log.Println("插入数据失败",err)
    }

}

//利用SQL语句特点批量插入数据，每次启动程序都全新扫描文件系统，不再保存旧文件信息。
func walkdirback3(path string, d fs.DirEntry, err error) error {
    info,_:=d.Info()
    absPath,_:=filepath.Abs(path)

    if info == nil {
        return nil
    }

    //只对普通文件进行处理
    if d.Type() & os.ModeType !=0 || 
    strings.HasPrefix(path, "/proc/") ||
    strings.HasPrefix(path, "/dev/") ||
    strings.HasPrefix(path, "/sys/") ||
    strings.HasPrefix(path, "/run/") ||
    strings.HasPrefix(path, "/swp/") || 

    //去除小文件，默认去除大小1000字节以下文件
    info.Size()< minSize ||

    //如果文件的最后修改时间离现在不到一定时间可能是日志文件也不处理
    info.ModTime().Add(time.Second*time.Duration(delay)).After(time.Now()) {
        return nil
    }

    //如果数据库中没有保存过该文件则插入该文件-----------

    md5Str := md5File(absPath)
    linkCount := getFileLinkCount(absPath)

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

//取文件的硬链接数，如果硬链接数返回0，说明执行出错
func getFileLinkCount(fileAbsName string) int {
    // 使用 os.Stat() 获取文件状态信息
    fileInfo, err := os.Stat(fileAbsName)
    if err != nil {
        log.Printf("Error getting file info: %s\n", err)
        return 0
    }

    // 强制类型转换到 syscall.Stat_t
    sysStat := fileInfo.Sys().(*syscall.Stat_t)
    return int(sysStat.Nlink)
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
    if err := os.Link(sourcePath,linkPath); err != nil {
        return errors.New("创建链接出错:" + err.Error())
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
    var fileSize int  //每个文件的大小
    repeatFileNum :=0  //重复文件个数
    repeatGroupNum := len(md5Strs) //有多少组重复文件
    fmt.Println(md5Strs,len(md5Strs))
    zipSize =int64(0) 
    for _,value := range md5Strs {
        sqlStr:=`select abspath,size from tfile where md5=? order by mtime`
        rows,err := db.Query(sqlStr,value) 
        if err != nil {
            return err
        }

        //取第一条记录,也就是时间最老的文件做为基准文件，其它文件均和其做硬链接
        var sourcePath string
        rows.Next()
        err = rows.Scan(&sourcePath,&fileSize) //第一条记录的filesize无用抛弃
        if err != nil{
            return err
        }
        repeatFileNum = repeatFileNum + 1

        //遍历其它文件并和基准文件做硬链接
        var dPath string
        for rows.Next() {
            err := rows.Scan(&dPath,&fileSize)
            if err != nil {
                rows.Close()
                return err
            }

            //找到一个文件重复文件数就加1
            repeatFileNum = repeatFileNum + 1
            //计算节省的磁盘空间是否真正删除了数据是无法从文件信息中统计出来的，
            //只能在执行删除之前根据inode的引用数是否为1，从而判断是否真正删除了数据
            if getFileLinkCount(dPath) == 1{
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
        rows.Close()
    }
    if isOnlyScan == "yes" {
        fmt.Println("数据重复文件个数为:",repeatFileNum,",共:",repeatGroupNum,"组重复文件，节省:",zipSize/1000000,"M磁盘空间(估算数据仅供参考)")
    }else if isOnlyScan == "no"{

        fmt.Println("数据重复文件个数为:",repeatFileNum,",共:",repeatGroupNum,"组重复文件，节省:",zipSize/1000000,"M磁盘空间")
    }
    return nil
}
