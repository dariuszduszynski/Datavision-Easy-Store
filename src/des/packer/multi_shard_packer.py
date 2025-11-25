"""
Multi-shard packer with big file support.
"""
from des.core.writer import DesWriter
from des.core.constants import DEFAULT_BIG_FILE_THRESHOLD

class MultiShardPacker:
    """
    Packer z obsługą big files.
    """
    
    def __init__(self, pod_index, shard_assignment, config, db, s3_source, s3_archive):
        # ... existing init ...
        
        # Big file configuration
        self.big_file_threshold = config.get(
            'big_file_threshold', 
            DEFAULT_BIG_FILE_THRESHOLD
        )
    
    def get_writer_for_shard(self, shard_id: int) -> DesWriter:
        """
        Lazy init writer dla sharda z S3 storage dla big files.
        """
        if shard_id not in self.my_shards:
            raise ValueError(f"Shard {shard_id} not assigned to this pod!")
        
        today = date.today().isoformat()
        
        # Nowy dzień? Zamknij wszystkie stare writers
        if self.current_date != today:
            if self.current_date is not None:
                self.finalize_all_shards()
            self.current_date = today
        
        # Lazy init dla tego sharda
        if shard_id not in self.writers:
            local_path = f"/tmp/shard_{shard_id:02d}.des"
            
            # Writer z S3 storage dla big files
            self.writers[shard_id] = DesWriter(
                path=local_path,
                big_file_threshold=self.big_file_threshold,
                external_storage=self.s3_archive  # S3 client
            )
            
            # Musimy ustawić bucket na S3 client
            self.s3_archive.bucket = self.archive_bucket
            
            print(f"Initialized writer for shard {shard_id} (threshold: {self.big_file_threshold} bytes)")
        
        return self.writers[shard_id]
    
    def process_batch(self, files: List[FileMetadata]):
        """
        Pakuje pliki z automatyczną detekcją big files.
        """
        for file in files:
            try:
                # 1. Pobierz z S3 source
                data = self.s3_source.get_object(
                    Bucket=self.source_bucket,
                    Key=file.s3_path
                )['Body'].read()
                
                # 2. Dodaj do writera (automatycznie wykryje czy big file)
                writer = self.get_writer_for_shard(file.shard_id)
                writer.add_file(
                    name=file.snowflake_name,
                    data=data,
                    meta={
                        'original_path': file.s3_path,
                        'file_id': file.file_id,
                        'shard_id': file.shard_id,
                        'archived_at': datetime.utcnow().isoformat()
                    }
                )
                
                # 3. Update DB status
                # Ścieżka zawsze wskazuje na DES (nawet jeśli dane external)
                des_path = f"{self.current_date}/shard_{file.shard_id:02d}.des:{file.snowflake_name}"
                
                self.db.execute("""
                    UPDATE files_metadata
                    SET 
                        status = 'archived',
                        des_path = %s,
                        archived_at = NOW()
                    WHERE file_id = %s
                """, [des_path, file.file_id])
                
            except Exception as e:
                self.handle_error(file.file_id, e)