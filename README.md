# zstd_wrapper
package to convert the zst compressed files to ZIP, TAR, TXT, JSON or to Pandas Dataframe


**How to use:**

- Download the package.
- Use the function `convert_zst(input_path, output_path, output_filetype=[zip|tar|txt|json], max_window_size=2147483648, stream_reader_size=16384)` to convert the ZST  compressed file.
- Use `zst_to_dataframe(input_path, zst_content_type=[json|None], max_window_size=2147483648)` to create the ZST compressed file to dataframe
- `max_window_size` is the memory size provided to the zstandard decompressor.

Script has been successfully tested on Pushshift reddit dumps.

