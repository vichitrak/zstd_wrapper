import io
import os
import json
import errno
import pandas
import zipfile
import tarfile
import tempfile
import zstandard

def extract_zst(input_path, output_path, output_filetype='.txt', max_window_size=2147483648, stream_reader_size=16384):
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), input_path)
    if not os.path.isdir(output_path):
        raise NotADirectoryError(f'{output_path} is not a directory!')
    if not os.path.exists(output_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), output_path)
    
    if zstandard is None:
        raise ImportError("zstandard has not been installed")
    if pandas is None:
        raise ImportError("pandas has not been installed")
            
    dctx = zstandard.ZstdDecompressor(max_window_size=max_window_size)
    
    full_output_path = os.path.join(output_path, os.path.basename(input_path) + '_decomp.txt')
    
    with tempfile.TemporaryFile() as temp_output:
        with open(input_path, "rb") as compressed_input:
            stream_reader = dctx.stream_reader(compressed_input)
            while True:
                chunk = stream_reader.read(stream_reader_size)
                if not chunk:
                    break
                temp_output.write(chunk)
        temp_output.seek(0)
        
        tarred_file_name = os.path.basename(input_path)[:-4]
        
        if output_filetype=='zip':
            full_output_path = os.path.join(output_path, os.path.basename(input_path) + '_decomp.zip')
            print("f", full_output_path)
            with zipfile.ZipFile(full_output_path, mode='w') as zipf:
                zipf.writestr(tarred_file_name, temp_output.read())
            temp_output.close()
        
        if output_filetype=='tar':
            temp_output.seek(0)
            byte_array = temp_output.read()
            
            full_output_path = os.path.join(output_path, os.path.basename(input_path) + '_decomp.tar.gz')
            tarred_file_name = os.path.basename(input_path)[:-4]

            tarfile_info = tarfile.TarInfo(tarred_file_name)
            tarfile_info.size = len(byte_array)

            tar_file = tarfile.open(full_output_path, "w:gz")
            tar_file.addfile(tarfile_info, io.BytesIO(bytearray(byte_array)))
            tar_file.close()
        
        if output_filetype=='txt':
            full_output_path = os.path.join(output_path, os.path.basename(input_path) + '_decomp.txt')
            with open(full_output_path, mode='wb') as text_file:
                text_file.write(temp_output.read())
            
        if output_filetype=='json':
            full_output_path = os.path.join(output_path, os.path.basename(input_path) + '_decomp.json')
            dict_list = []
            for item in temp_output.read().split(b'\n'):
                if item != b'':
                    bytes_to_dict = json.loads(item.decode("UTF-8"))
                    sorted_item = dict(sorted(bytes_to_dict.items()))   
                    dict_list.append(sorted_item)

            with open(full_output_path, mode='w') as json_file:
                json.dump(dict_list, json_file)


def zst_to_dataframe(input_path, zst_content_type='json', max_window_size=2147483648):
    def create_zstd_generator(input_path):
        dctx = zstandard.ZstdDecompressor(max_window_size=max_window_size)
        with (zstandard.open(input_path, "rb", dctx=dctx) as compressed_input, 
                                        io.TextIOWrapper(compressed_input) as io_wrapper):
            for line in io_wrapper:
                yield line

    wrapper = create_zstd_generator(input_path)
    lst = list()
    if zst_content_type=='json':
        for line in wrapper:
            lst.append(json.loads(line))
    else:
        for line in wrapper:
            lst.append(line)
    df = pandas.DataFrame(lst)
    return df
