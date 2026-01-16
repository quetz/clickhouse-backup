package storage

import (
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archives"
	"github.com/rs/zerolog/log"
	"sort"
	"strings"
	"time"
)

func GetBackupsToDeleteRemote(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		// sort backup ascending
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].UploadDate.After(backups[j].UploadDate)
		})
		// KeepRemoteBackups should respect incremental backups sequences and don't deleteKey required backups
		// fix https://github.com/Altinity/clickhouse-backup/issues/111
		// fix https://github.com/Altinity/clickhouse-backup/issues/385
		// fix https://github.com/Altinity/clickhouse-backup/issues/525
		deletedBackups := make([]Backup, len(backups)-keep)
		copied := copy(deletedBackups, backups[keep:])
		if copied != len(backups)-keep {
			log.Warn().Msgf("copied wrong items from backup list expected=%d, actual=%d", len(backups)-keep, copied)
		}
		keepBackups := make([]Backup, keep)
		copied = copy(keepBackups, backups[:keep])
		if copied != keep {
			log.Warn().Msgf("copied wrong items from backup list expected=%d, actual=%d", keep, copied)
		}
		var findRequiredBackup func(b Backup)
		findRequiredBackup = func(b Backup) {
			if b.RequiredBackup != "" {
				for i, deletedBackup := range deletedBackups {
					if b.RequiredBackup == deletedBackup.BackupName {
						deletedBackups = append(deletedBackups[:i], deletedBackups[i+1:]...)
						findRequiredBackup(deletedBackup)
						break
					}
				}
			}
		}
		for _, b := range keepBackups {
			findRequiredBackup(b)
		}
		// remove from old backup list backup with UploadDate `0001-01-01 00:00:00`, to avoid race condition for multiple shards copy
		// fix https://github.com/Altinity/clickhouse-backup/issues/409
		i := 0
		for _, b := range deletedBackups {
			if b.UploadDate != time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC) {
				deletedBackups[i] = b
				i++
			}
		}
		deletedBackups = deletedBackups[:i]
		return deletedBackups
	}
	return []Backup{}
}

type compressedArchive struct {
	compression archives.Compression
	archival    archives.Archival
}

func getArchiveWriter(format string, level int) (*compressedArchive, error) {
	archival := archives.Tar{}
	var compression archives.Compression

	switch format {
	case "tar":
		return &compressedArchive{archival: archival}, nil
	case "lz4":
		compression = archives.Lz4{CompressionLevel: level}
	case "bzip2", "bz2":
		compression = archives.Bz2{CompressionLevel: level}
	case "gzip", "gz":
		compression = archives.Gz{CompressionLevel: level, Multithreaded: true}
	case "sz":
		compression = archives.Sz{}
	case "xz":
		compression = archives.Xz{}
	case "br", "brotli":
		compression = archives.Brotli{Quality: level}
	case "zstd":
		compression = archives.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level))}}
	default:
		return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
	}

	return &compressedArchive{compression: compression, archival: archival}, nil
}

func getArchiveReader(format string) (*compressedArchive, error) {
	archival := archives.Tar{}
	var compression archives.Compression

	switch format {
	case "tar":
		return &compressedArchive{archival: archival}, nil
	case "lz4":
		compression = archives.Lz4{}
	case "bzip2", "bz2":
		compression = archives.Bz2{}
	case "gzip", "gz":
		compression = archives.Gz{Multithreaded: true}
	case "sz":
		compression = archives.Sz{}
	case "xz":
		compression = archives.Xz{}
	case "br", "brotli":
		compression = archives.Brotli{}
	case "zstd":
		compression = archives.Zstd{}
	default:
		return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
	}

	return &compressedArchive{compression: compression, archival: archival}, nil
}

func checkArchiveExtension(ext, format string) bool {
	if (format == "gz" || format == "gzip") && ext != ".gz" && ext != ".gzip" {
		return false
	}
	if (format == "bz2" || format == "bzip2") && ext != ".bz2" && ext != ".bzip2" {
		return false
	}
	if (format == "br" || format == "brotli") && ext != ".br" && ext != ".brotli" {
		return false
	}
	if strings.HasSuffix(ext, format) {
		return true
	}
	if (format == "gz" || format == "gzip") && (ext == ".gz" || ext == ".gzip") {
		return true
	}
	if (format == "bz2" || format == "bzip2") && (ext == ".bz2" || ext == ".bzip2") {
		return true
	}
	if (format == "br" || format == "brotli") && (ext == ".br" || ext == ".brotli") {
		return true
	}
	return false
}
