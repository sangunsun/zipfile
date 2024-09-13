// +build windows

//Copyright 2024 sangunsun All Rights Reserved
//创建硬链接和获取硬链接数
//windows下golang不支持，只能通过syscall调用windowsAPI实现
package main
import(
    "errors"
    "fmt"
    "syscall"
    "unsafe"
)

var (
    modkernel32 = syscall.NewLazyDLL("kernel32.dll")
    procGetFileInformationByHandleEx = modkernel32.NewProc("GetFileInformationByHandleEx")
)

type FILE_STANDARD_INFO struct {

    AllocationSize uint64
    EndOfFile      uint64
    NumberOfLinks  uint32
    DeletePending  uint8
    Directory      uint8
}

const (
    FileStandardInfo = 1
)

func hardLinkFile(sourcePath string,linkPath string) error {
        if err :=createHardLink(sourcePath,linkPath);err != nil{
            return errors.New("创建链接出错:" + err.Error())
        }
    return nil
}

func createHardLink(existingFile, newLink string) error {

    // 将Go字符串转换为Windows API所需的字符串格式
    existingFilePtr, err := syscall.UTF16PtrFromString(existingFile)
    if err != nil {
        return err
    }
    newLinkPtr, err := syscall.UTF16PtrFromString(newLink)
    if err != nil {
        return err
    }

    // 创建硬链接
    err = syscall.CreateHardLink(newLinkPtr, existingFilePtr, uintptr(0))
    if err != nil {
        return fmt.Errorf("CreateHardLink failed: %v", err)
    }

    return nil
}

func getHardLinks(fileName string) int {
    fd, err := syscall.CreateFile(syscall.StringToUTF16Ptr(fileName),
    syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, 0, 0)
    if fd == syscall.InvalidHandle {

        fmt.Printf("无法打开文件: %v\n", err)
        return 0
    }
    defer syscall.CloseHandle(fd)

    var standardInfo FILE_STANDARD_INFO
    infoSize := uint32(unsafe.Sizeof(standardInfo))
    err = getFileInformationByHandleEx(fd, FileStandardInfo, (*byte)(unsafe.Pointer(&standardInfo)), infoSize)
    if err != nil {

        fmt.Printf("GetFileInformationByHandleEx failed: %v\n", err)
        return 0
    }

    //fmt.Printf("文件 '%s' 的硬链接数为: %d\n", fileName, standardInfo.NumberOfLinks)
    return int(standardInfo.NumberOfLinks)
}

func getFileInformationByHandleEx(hFile syscall.Handle, FileInformationClass uint32, lpFileInformation *byte, dwBufferSize uint32) (err error) {

    r0, _, e1 := syscall.Syscall6(procGetFileInformationByHandleEx.Addr(), 4, uintptr(hFile), uintptr(FileInformationClass), uintptr(unsafe.Pointer(lpFileInformation)), uintptr(dwBufferSize), 0, 0)
    if r0 == 0 {

        if e1 != 0 {

            err = fmt.Errorf("调用失败: %v", e1)
        } else {

            err = fmt.Errorf("调用失败: 未知错误")
        }
    }
    return
}
