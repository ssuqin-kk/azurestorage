#pragma once

#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/filestream.h>
#include <cpprest/containerstream.h>
#include "AzureUtil.hpp"
#include<map>

// doc:https://docs.microsoft.com/zh-cn/azure/storage/blobs/storage-c-plus-plus-how-to-use-blobs
typedef std::string String;

class AzureStorageBlob {

private:
	AzureStorageBlob() {}
	AzureStorageBlob(const AzureStorageBlob&) {}
	AzureStorageBlob& operator=(const AzureStorageBlob&) {}
	azure::storage::cloud_blob_client BlobClient;       
	std::string LastError = "";
	std::map<std::string, azure::storage::cloud_blob_container> Containers;

	void GetContainer(String containerName) {
		try {
			if (Containers.find(containerName) == Containers.end()) {
				std::wstring wContainerName;
				AzureUtil::StringToWString(containerName, wContainerName);
				azure::storage::cloud_blob_container container = BlobClient.get_container_reference(wContainerName);
				container.create_if_not_exists();
				Containers[containerName] = container;
			}
		} catch (const std::exception& e) {
			LastError = e.what();
		}
	}
	
public:
	 std::string GetStorageConnectionString(String storageCount, String storageAccountKey, String protocol = "https") {
		 return "DefaultEndpointsProtocol=" + protocol + ";AccountName=" + storageCount + ";AccountKey=" + storageAccountKey;
	}

	static AzureStorageBlob& GetInstance() {
		static AzureStorageBlob Instance;
		return Instance;
	}


	bool Initial(String storageConnectionString) {
		try {

			std::wstring wStorageConnectionString;
			AzureUtil::StringToWString(storageConnectionString, wStorageConnectionString);

			// Retrieve storage account from connection string.
			azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(wStorageConnectionString);
			
			// Create the blob client.
			BlobClient = storage_account.create_cloud_blob_client();

		} catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool UploadFromStream(String containerName, String blockBlob, String filePath) {

		try {
			GetContainer(containerName);
			
			std::wstring wBlockBlob;
			std::wstring wFilePath;

			AzureUtil::StringToWString(blockBlob, wBlockBlob);
			AzureUtil::StringToWString(filePath, wFilePath);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);

			concurrency::streams::istream input_stream = concurrency::streams::file_stream<uint8_t>::open_istream(wFilePath).get();
			blockBlob.upload_from_stream(input_stream);
			input_stream.close().wait();
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool UploadText(String containerName, String blockBlob, String content) {
		try {
			GetContainer(containerName);

			std::wstring wBlockBlob;
			std::wstring wContent;

			AzureUtil::StringToWString(blockBlob, wBlockBlob);
			AzureUtil::StringToWString(content, wContent);

			azure::storage::cloud_block_blob blob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			blob.upload_text(wContent);
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}

	bool DownloadToStream(String containerName, String blockBlob,
		concurrency::streams::container_buffer<std::vector<uint8_t>> &buffer,
		unsigned long long offset = -1, unsigned long long length = -1) {
		
		try {

			GetContainer(containerName);

			std::wstring wBlockBlob;
			AzureUtil::StringToWString(blockBlob, wBlockBlob);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			concurrency::streams::ostream output_stream(buffer);

			if (offset == -1) {
				blockBlob.download_to_stream(output_stream);
			}
			else {
				blockBlob.download_range_to_stream(output_stream, offset, length);
			}
		} catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}

	bool DownloadToFile(String containerName, String blockBlob, String filePath) {
		try {
			GetContainer(containerName);

			std::wstring wBlockBlob;
			std::wstring wFilePath;

			AzureUtil::StringToWString(blockBlob, wBlockBlob);
			AzureUtil::StringToWString(filePath, wFilePath);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
			concurrency::streams::ostream output_stream(buffer);
			blockBlob.download_to_stream(output_stream);

			std::ofstream outfile(wFilePath, std::ofstream::binary);
			std::vector<unsigned char>& data = buffer.collection();
			outfile.write((char *)&data[0], buffer.size());
			outfile.close();
		} catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool DeleteBlob(String containerName, String blockBlob) {
		try {
			GetContainer(containerName);

			std::wstring wBlockBlob;
			AzureUtil::StringToWString(blockBlob, wBlockBlob);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			blockBlob.delete_blob();
		} catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	int FindBlockBlob(String containerName, String blockBlob) {
		int ret = -1;
		try {
			GetContainer(containerName);

			std::wstring wBlockBlob;
			AzureUtil::StringToWString(blockBlob, wBlockBlob);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			ret = (blockBlob.exists() ? 1 : 0);
		} catch (const std::exception& e) {
			LastError = e.what();
		}
		return ret;
	}


	unsigned long long GetBlockBlobSize(String containerName, String blockBlob) {
		unsigned long long size = 0;

		try {
			GetContainer(containerName);

			std::wstring wBlockBlob;
			AzureUtil::StringToWString(blockBlob, wBlockBlob);

			azure::storage::cloud_block_blob blockBlob = Containers[containerName].get_block_blob_reference(wBlockBlob);
			blockBlob.download_attributes();
			azure::storage::cloud_blob_properties properties = blockBlob.properties();
			size = properties.size();
		} catch (const std::exception& e) {
			LastError = e.what();
			size = -1;
		}
		return size;
	}

	String GetLastError() {
		return LastError;
	}

	void Dispose() {
		Containers.clear();
	}
};