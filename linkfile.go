// +build !windows

//创建硬链接和获取硬链接数
//非windows下golang直接支持创建硬链接和获取硬链接数
package main
import(
    "os"
    "errors"
    "syscall"
    "log"
)

func hardLinkFile(sourcePath string,linkPath string) error {
    if err := os.Link(sourcePath,linkPath); err != nil {
        return errors.New("创建硬链接出错:" + err.Error())
    }
    return nil
}

//取文件的硬链接数，如果硬链接数返回0，说明执行出错
func getHardLinks(fileAbsName string) int {
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
