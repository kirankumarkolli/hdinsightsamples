Add-Type -assembly "System.IO.Compression.FileSystem"

$sqoopHomeDir = (Get-Item env:SQOOP_HOME).Value
$mysqlPkgName = "mysql-connector-java-" + (Get-Date).Ticks + ".zip"
$mysqlPkgZip = join-path $sqoopHomeDir -ChildPath $mysqlPkgName
$sqoopLibsFolder = join-path -Path $sqoopHomeDir -ChildPath lib;

$mySqlUrl = "https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.0.8.zip"
Invoke-WebRequest $mySqlUrl -OutFile $mysqlpkgZip
Write-output "Downloaded $mySqlUrl to $mysqlpkgZip"

$sqoopFilter = { param($archiveEntry) $archiveEntry.Name -match ".*-bin.jar|COPYING" }
$copySqoopFile = { param($archiveEntry, $targetDir)
                    $targetFile = join-path -Path $targetDir -ChildPath $archiveEntry.Name
                    Write-output "Copying $archiveEntry to $targetFile"
                    [System.IO.Compression.ZipFileExtensions]::ExtractToFile($archiveEntry, $targetFile, $True)
                }

# Not diposed but fine for this usecase
$mysqlpkgZipArchive = [system.io.compression.ZipFile]::OpenRead($mysqlpkgZip);


$sqoopfiles = $mysqlpkgZipArchive.Entries | ? { &$sqoopFilter $_ -eq $True } 
$sqoopfiles | % { &$copySqoopFile $_ $sqoopLibsFolder } 

if ($sqoopfiles.Count -ne 2)
{
    throw "MySql connector setup failed";
}
