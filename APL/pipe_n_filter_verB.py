import threading
import sys
import os.path
import time
from copy import copy
from lockfile import FileLock
from datetime import datetime
from random import randint

exit_flag = False
list_pipe = []
list_filter = []


class Filter(threading.Thread):
    def __init__(self, mode):
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.busy = False
        self.output = None
        self.processing = ""
        self.mode = mode
        self._filter_input = None
        self.prev_pipe = None
        self.next_pipe = None
        self.running = True
        self.raise_flag = False

    def stop(self):
        self._stop.set()
        self.running = False

    def insert_input(self, filter_input):
        self._filter_input = filter_input[2]
        self.processing = filter_input[1]
        self.mode = filter_input[0]

    def operate(self):
        print "Operating Filter"

    def run(self):
        while self.running and not exit_flag:
            if not self.busy:
                temp = self.prev_pipe.pop_storage(self.mode)
                if temp != "code_1" and temp != "code_0":
                    self.insert_input(temp)
                    self.operate()
                elif temp == "code_1":
                    self.raise_flag = True
                    while self.raise_flag and not exit_flag:
                        time.sleep(2)


class FilterInput(Filter):
    __filename = ""
    __plain_data = ""

    def insert_input(self, filter_input):
        self.mode = filter_input[0]
        self.processing = filter_input[1]
        self.__filename = filter_input[2]

    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterInput "
        self.output = None

        lock_file = FileLock(self.__filename)
        status = lock_file.is_locked()

        while status:
            status = lock_file.is_locked()
        if os.path.isfile(self.__filename):
            file_open = open(self.__filename, "rb")
            self.__plain_data = file_open.read()
            file_open.close()

            if len(self.__plain_data) < 1:
                print "Error 400! File input is empty. [File - " + self.__filename + \
                  "]\n--Terminating ID " + self.processing + " in FilterInput."
            else:
                self.next_pipe.add_storage([self.mode, self.processing, [self.__plain_data, self.__filename]])

        self.busy = False


class Tree:
    def __init__(self):
        self.status = 1
        self.left = None
        self.right = None
        self.count = 0
        self.alphabet = ""

    def redeclare(self, alpha, count):
        self.status = 0
        self.left = None
        self.right = None
        self.count = count
        self.alphabet = alpha


class FilterConstruct(Filter):
    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterConstruct"
        input_text = self._filter_input[0]
        temp_count = []
        temp_result = []
        temp_list = list(set(input_text))
        for i in temp_list:
            count = input_text.count(i)
            temp_count.append(count)
        temp_count2 = copy(temp_count)
        temp_list2 = []
        for i in range(0, len(temp_list)):
            min_index = temp_count.index(min(temp_count))
            temp_tree = Tree()
            temp_tree.redeclare(temp_list[min_index], temp_count[min_index])
            temp_result.append(temp_tree)
            temp_list2.append(temp_list[min_index])
            del temp_count[min_index]
            del temp_list[min_index]
        temp_count2.sort()
        self.next_pipe.add_storage(
            [self.mode, self.processing, [temp_result, temp_count2, input_text, temp_list2, self._filter_input[1]]])
        self.busy = False


class FilterHuffman(Filter):
    __dict_converted = {}

    def get_result(self, start, head):
        if head.status == 0:
            temp_result = start
            temp_alphabet = head.alphabet
            self.__dict_converted[temp_alphabet] = temp_result
            return
        else:
            start1 = start + "0"
            start2 = start + "1"
            self.get_result(start1, head.left)
            self.get_result(start2, head.right)
            return

    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterHuffman"
        list_alphabet = self._filter_input[0]
        list_count = self._filter_input[1]
        alpha_count = len(list_alphabet)
        while alpha_count > 1:
            temp_left = list_alphabet.pop(0)
            temp_right = list_alphabet.pop(0)
            list_count.pop(0)
            list_count.pop(0)

            temp_parent = Tree()
            temp_parent.left = temp_left
            temp_parent.right = temp_right
            temp_count = temp_left.count + temp_right.count
            temp_parent.count = temp_count
            list_count.append(temp_count)
            list_count.sort()
            temp_index = list_count.index(temp_count)
            list_alphabet = list_alphabet[:temp_index] + [temp_parent] + list_alphabet[temp_index:]
            alpha_count -= 1

        self.get_result("", list_alphabet.pop(0))
        result = copy(self.__dict_converted)
        self.__dict_converted.clear()
        self.next_pipe.add_storage([self.mode, self.processing, [result,
                                    self._filter_input[2], self._filter_input[3], self._filter_input[4]]])
        self.busy = False


class FilterEncode(Filter):
    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterEncode"
        result = ""
        dict_converted = self._filter_input[0]
        plain_text = self._filter_input[1]
        rank_alphabet = self._filter_input[2]
        filename = self._filter_input[3]

        for i in plain_text:
            result += dict_converted[i]

        count_zero = len(result) % 8
        result += '0' * count_zero
        final_result = ""
        it = 0
        size = len(result)
        while it < size:
            temp = result[it:it+8]
            temp = int(temp, 2)
            final_result += chr(temp)
            it += 8

        length = len(plain_text)
        length_text = str(len(final_result))

        result = str(length) + "_"

        for i in rank_alphabet:
            temp = dict_converted[i]
            length = len(dict_converted[i])
            count_zero = length % 8
            if count_zero != 0:
                temp = '0' * count_zero + temp
            temp = int(temp, 2)
            result += str(length) + "-" + str(temp) + "" + "/|" + i
            if rank_alphabet.index(i) < len(rank_alphabet) - 1:
                result += "=*"

        temp = length_text + "_"
        temp2 = final_result + result
        temp += temp2

        self.next_pipe.add_storage([self.mode, self.processing, [filename, temp]])
        self.busy = False


class FilterWrite(Filter):
    def operate(self):
        self.busy = True
        filename = self._filter_input[0]

        data = self._filter_input[1]
        print "--ID " + self.processing + " Running FilterWrite"
        if self.mode == "encode":
            output_file = filename + ".d2f"

        else:
            temp_pos = filename.rfind('.')
            output_file = filename[:temp_pos]

        lock_file = None

        if os.path.isfile(output_file):
            lock_file = FileLock(output_file)
            status = lock_file.is_locked()
            while status:
                status = lock_file.is_locked()
            lock_file.acquire()

        file_open = open(output_file, "wb")
        file_open.write(data)
        file_open.close()
        if lock_file:
            lock_file.release()

        print "--ID " + self.processing + " Finish"
        self.busy = False


class FilterDictionary(Filter):
    def operate(self):
        # noinspection PyBroadException
        try:
            self.busy = True
            print "--ID " + self.processing + " Running FilterDictionary"
            temp_text = self._filter_input[0]
            temp_pos = temp_text.find('_')
            size = int(temp_text[:temp_pos])
            data = temp_text[temp_pos+1:]
            encoded_data = data[:size]
            temp = data[size:]

            temp_pos = temp.find('_')
            size = int(temp[:temp_pos])
            temp = temp[temp_pos + 1:]
            temp_list = temp.split("=*")
            dict_binary = {}
            for i in range(0, len(temp_list)):
                temp = temp_list[i]
                temp_list2 = temp.split("/|")
                temp_list3 = temp_list2[0].split('-')
                temp_binary = "{0:b}".format(int(temp_list3[1]))
                temp_binary = temp_binary.replace(" ", "")
                temp_length = int(temp_list3[0])
                if temp_length < len(temp_binary):
                    range_binary = len(temp_binary) - temp_length
                    bin_val = temp_binary[range_binary:]
                elif temp_length > len(temp_binary):
                    range_binary = temp_length - len(temp_binary)
                    bin_val = "0" * range_binary
                    bin_val += temp_binary
                else:
                    bin_val = temp_binary

                dict_binary[bin_val] = temp_list2[1]

            self.next_pipe.add_storage(
                    [self.mode, self.processing, [self._filter_input[1], size, dict_binary, encoded_data]])
            self.busy = False
        except:
            print "Error 500! File input either corrupt or invalid. [File - " + self._filter_input[1] + \
                  "]\n--Terminating ID " + self.processing + " in FilterDictionary."
            self.busy = False


class FilterConData(Filter):
    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterConData"
        size = self._filter_input[1]
        dict_binary = self._filter_input[2]
        text = str(self._filter_input[3])
        temp_result = ""
        for i in text:
            temp_result += "{0:8b}".format(ord(i))

        temp_result = temp_result.replace(" ", "0")
        self.next_pipe.add_storage([self.mode, self.processing,
                                    [self._filter_input[0], size, dict_binary, temp_result]])
        self.busy = False


class FilterDecode(Filter):
    def operate(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterDecode"
        size = self._filter_input[1]
        dict_binary = self._filter_input[2]
        temp_result = str(self._filter_input[3])
        result = ""
        count = 0
        temp = ""
        found = False

        for i in temp_result:
            temp += i
            if temp in dict_binary:
                found = True
                count += 1
                result += dict_binary[temp]

            if count == size:
                break

            if found:
                found = False
                temp = ""

        self.next_pipe.add_storage([self.mode, self.processing, [self._filter_input[0], result]])
        self.busy = False


class Pipe:
    def __init__(self):
        self.__storage_data = []

    def check_storage(self):
        return len(self.__storage_data)

    def add_storage(self, data):
        self.__storage_data.append(data)

    def pop_storage(self, mode):
        size = len(self.__storage_data)
        if size > 0:
            temp = self.__storage_data[0]
            if str(mode).strip() != str(temp[0]).strip():
                return "code_1"
            else:
                temp_data = self.__storage_data.pop(0)
                return temp_data
        else:
            return "code_0"


class Checker(threading.Thread):
    @staticmethod
    def construct_encode():
        global list_filter

        for i in range(0, 5):
            list_filter[i].mode = "encode"
            list_filter[i].raise_flag = False

    @staticmethod
    def construct_decode():
        global list_filter

        for i in range(4, 8):
            list_filter[i].mode = "decode"
            list_filter[i].raise_flag = False

        list_filter[0].mode = "decode"
        list_filter[0].raise_flag = False

    def run(self):
        while not exit_flag:
            time.sleep(2)
            if list_filter[0].raise_flag:
                while True:
                    if list_pipe[1].check_storage() == 0 and list_pipe[2].check_storage() == 0 \
                            and list_pipe[3].check_storage() == 0 and list_pipe[4].check_storage() == 0:
                        if list_filter[0].mode == "encode":
                            self.construct_decode()
                            break
                        else:
                            self.construct_encode()
                            break


class Main:
    def __init__(self):
        global list_filter
        global list_pipe
        for i in range(0, 5):
            list_pipe.append(Pipe())

        list_filter.append(FilterInput("encode"))
        list_filter.append(FilterConstruct("encode"))
        list_filter.append(FilterHuffman("encode"))
        list_filter.append(FilterEncode("encode"))
        list_filter.append(FilterWrite("encode"))
        list_filter.append(FilterDictionary("decode"))
        list_filter.append(FilterConData("decode"))
        list_filter.append(FilterDecode("decode"))

        list_filter[0].prev_pipe = list_pipe[0]
        list_filter[0].next_pipe = list_pipe[1]
        list_filter[1].prev_pipe = list_pipe[1]
        list_filter[1].next_pipe = list_pipe[2]
        list_filter[2].prev_pipe = list_pipe[2]
        list_filter[2].next_pipe = list_pipe[3]
        list_filter[3].prev_pipe = list_pipe[3]
        list_filter[3].next_pipe = list_pipe[4]
        list_filter[4].prev_pipe = list_pipe[4]
        list_filter[5].prev_pipe = list_pipe[1]
        list_filter[5].next_pipe = list_pipe[2]
        list_filter[6].prev_pipe = list_pipe[2]
        list_filter[6].next_pipe = list_pipe[3]
        list_filter[7].prev_pipe = list_pipe[3]
        list_filter[7].next_pipe = list_pipe[4]

        for i in range(0, 8):
            list_filter[i].setDaemon(True)
            list_filter[i].start()

        checker = Checker()
        checker.setDaemon(True)
        checker.start()

    @staticmethod
    def run():
        global exit_flag
        global list_pipe
        print("Filename with format:")
        name_file = sys.stdin.readline().strip()
        while name_file != "EXIT":
            temp_list = name_file.split("||")
            for i in temp_list:
                temp_pos = i.rfind("/")
                filename = i[:temp_pos]
                if "/e" not in i and "/d" not in i:
                    print "Error 400! No operation command detected. [File - " + i + "]"
                else:
                    if os.path.isfile(filename):
                        key = datetime.now().strftime('%Y%m%d%H%M%S') + str(randint(100, 999))
                        if "/e" in i:
                            mode = "encode"
                            list_pipe[0].add_storage([mode, key, filename])
                        elif ".d2f" == i[len(i)-6:len(i)-2]:
                            mode = "decode"
                            list_pipe[0].add_storage([mode, key, filename])
                        else:
                            print "Error 400! Invalid file format. [File - " + i + "]"
                    else:
                        print "Error 404! File not found. [File - " + i + "]"

            name_file = sys.stdin.readline().strip()

        exit_flag = True
        for i in range(0, 8):
            list_filter[i].join()

        for i in list_pipe:
            del i


run_main = Main()
run_main.run()
