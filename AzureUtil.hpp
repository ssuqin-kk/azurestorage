#pragma once

#include<Windows.h>
#include<string>
#include<fstream>
#include<vector>
#include <ctime>
#include<sstream>
#include<iomanip>

class AzureUtil {
public:

	static std::string ReadFile(const char * path)
	{
		std::ifstream t(path);
		return std::string((std::istreambuf_iterator<char>(t)),
			std::istreambuf_iterator<char>());
	}

	static bool StringToWString(const std::string str, std::wstring &wstr)
	{
#ifdef WIN32
		int nLen = (int)str.length();
		wstr.resize(nLen, L' ');

		int nResult = MultiByteToWideChar(CP_ACP, 0, (LPCSTR)str.c_str(), nLen, (LPWSTR)wstr.c_str(), nLen);

		if (nResult == 0)
			return false;

#endif

		return true;
	}

	//wstring高字节不为0，返回FALSE
	static bool WStringToString(const std::wstring wstr, std::string &str)
	{
#ifdef WIN32
		LPCWSTR ws = wstr.c_str();
		int wsLen = WideCharToMultiByte(CP_ACP, 0, ws, -1, NULL, 0, NULL, NULL);
		char* dst = new char[wsLen];
		int nResult = WideCharToMultiByte(CP_ACP, 0, ws, -1, dst, wsLen, NULL, NULL);
		if (nResult == 0)
		{
			delete[] dst;
			return false;
		}

		dst[wsLen - 1] = 0;
		str = dst;
		delete[] dst;
#endif
		return true;
	}

	static std::string ConvertUnixStampToUTC0Str(unsigned long time_date_stamp) 
	{
		std::time_t temp = time_date_stamp;
		std::tm* t = std::gmtime(&temp);
		std::stringstream ss;
		ss << std::put_time(t, "%Y-%m-%dT%H:%M:%SZ");
		return ss.str();
	}

	static std::string ConvertLocaltsToLocalTimeStr(unsigned long time_date_stamp) {
		std::time_t temp = time_date_stamp;
		std::tm* t = std::localtime(&temp);
		std::stringstream ss;
		ss << std::put_time(t, "%Y-%m-%dT%H:%M:%SZ");
		return ss.str();
	}

	static std::string AsciiCharPtrToUtf8(const char* pszOutBuffer)
	{
		DWORD dwNum = MultiByteToWideChar(CP_ACP, 0, pszOutBuffer, -1, NULL, 0);    // 返回原始ASCII码的字符数目       
		wchar_t* pwText = new wchar_t[dwNum];                                       // 根据ASCII码的字符数分配UTF8的空间
		ZeroMemory(pwText, 0, nLen * 2 + 2);

		MultiByteToWideChar(CP_UTF8, 0, pszOutBuffer, -1, pwText, dwNum);           // 将ASCII码转换成UTF8

		std::string retStr;

		WStringToString(pwText, retStr);

		delete[] pwText;
		pwText = NULL;

		return retStr;
	}
};