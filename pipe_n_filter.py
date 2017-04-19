import threading
import sys
import os.path
from copy import copy
from lockfile import FileLock
from datetime import datetime
import time

exit_flag = False


class Filter:
    def __init__(self):
        self.busy = False
        self.output = None
        self.processing = ""
        self.mode = ""
        self._filter_input = None

    def insert_input(self, filter_input):
        self._filter_input = filter_input[2]
        self.processing = filter_input[1]
        self.mode = filter_input[0]


class FilterInput(Filter):
    __filename = ""
    __plain_data = ""

    def insert_input(self, filter_input):
        self.mode = filter_input[0]
        self.__filename = filter_input[1]

    def run(self):
        self.busy = True
        key = datetime.now().strftime('%Y%m%d%H%M%S')
        self.processing = key
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
            self.output = [self.__plain_data, self.__filename]
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

    def run(self):
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
        self.output = [temp_result, temp_count2, input_text, temp_list2, self._filter_input[1]]
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

    def run(self):
        if self._filter_input:
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
            self.output = [result, self._filter_input[2], self._filter_input[3], self._filter_input[4]]
            self.busy = False


class FilterEncode(Filter):

    def run(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterEncode"
        result = ""
        dict_converted = self._filter_input[0]
        plain_text = self._filter_input[1]
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

        self.output = [len(plain_text), dict_converted, final_result, self._filter_input[2], self._filter_input[3]]
        self.busy = False


class FilterWrite(Filter):

    def run(self):
        self.busy = True
        if self.mode == "encode":
            length = self._filter_input[0]
            dict_converted = self._filter_input[1]
            converted_data = self._filter_input[2]
            rank_alphabet = self._filter_input[3]
            filename = self._filter_input[4]
            output_file = filename + ".d2f"
            temp = False
            lock_file = None

            print "--ID " + self.processing + " Running FilterWrite"
            if os.path.isfile(output_file):
                lock_file = FileLock(output_file)
                status = lock_file.is_locked()
                while status:
                    time.sleep(2)
                    status = lock_file.is_locked()

                lock_file.acquire()
                temp = True
            file_open = open(output_file, "wb")
            file_open.write(converted_data)
            file_open.close()
            if temp:
                lock_file.release()

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

            output_file = filename + ".d2c"
            if os.path.isfile(output_file):
                lock_file = FileLock(output_file)
                status = lock_file.is_locked()
                while status:
                    time.sleep(2)
                    status = lock_file.is_locked()
                lock_file = FileLock(output_file)
                lock_file.acquire()
                temp = True
            file_open = open(output_file, "wb")
            file_open.write(result)
            file_open.close()
            if temp:
                lock_file.release()

            if self.mode == "decode":
                filename = self._filter_input[0]
                converted_data = self._filter_input[0]
                temp_pos = filename.rfind('.d2f')
                filename = filename[:temp_pos]

                if os.path.isfile(filename):
                    lock_file = FileLock(filename)
                    status = lock_file.is_locked()
                    while status:
                        time.sleep(2)
                        status = lock_file.is_locked()

                    lock_file.acquire()
                    temp = True

                file_open = open(filename, "wb")
                file_open.write(converted_data)
                file_open.close()
                if temp:
                    lock_file.release()

        self.busy = False


class FilterDictionary(Filter):

    def run(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterDictionary"
        filename = self._filter_input[1]
        temp_pos = filename.rfind('.')
        filename = filename[:temp_pos]
        filename += ".d2c"
        temp = ""

        lock_file = FileLock(filename)
        status = lock_file.is_locked()

        while status:
            status = lock_file.is_locked()

        if os.path.isfile(filename):
            file_open = open(filename, "rb")
            temp = file_open.read()
            file_open.close()

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
            temp_length = int(temp_list3[0])
            if temp_length < len(temp_binary):
                range_binary = len(temp_binary) - temp_length
                bin_val = temp_binary[range_binary:]
            elif temp_length > len(temp_binary):
                range_binary = len(temp_binary) - temp_length
                bin_val = '0' * range_binary + temp_binary
            else:
                bin_val = temp_binary

            dict_binary[bin_val] = temp_list2[1]

        self.output = [filename, size, dict_binary, self._filter_input[0]]
        self.busy = False


class FilterDecode(Filter):

    def run(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterDecode"
        size = self._filter_input[1]
        dict_binary = self._filter_input[2]
        text = self._filter_input[3]
        temp_result = ""
        for i in text:
            print i
            temp_result += "{0:8b}".format(int(i))

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

        self.output = [self._filter_input[0], result]
        self.busy = False


class Pipe(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.__storage_data = []
        self.in_mode = ""
        self.prev_filter = None
        self.next_filter = None
        self.running = True

    def stop(self):
        self._stop.set()
        self.running = False

    def add_storage(self, data):
        self.__storage_data.append(data)

    def check_prev(self):
        if not self.prev_filter.busy and self.prev_filter.output and self.prev_filter.mode == self.in_mode:
            temp_value = self.prev_filter.output
            temp_mode = self.in_mode
            self.prev_filter.output = None
            temp_key = self.prev_filter.processing
            self.__storage_data.append([temp_mode, temp_key, temp_value])

    def check_next(self):
        size = len(self.__storage_data)
        if not self.next_filter.busy and size > 0:
            temp_data = self.__storage_data.pop(0)
            self.next_filter.insert_input(temp_data)
            self.next_filter.run()

    def run(self):
        while not exit_flag:
            if self.prev_filter:
                self.check_prev()
            if self.next_filter:
                self.check_next()

        self.running = False


def construct_pipeline():
    list_pipe = []
    list_filter = []
    for i in range(0, 8):
        list_pipe.append(Pipe())
        if i < 5:
            list_pipe[i].in_mode = "encode"
        else:
            list_pipe[i].in_mode = "decode"

    list_filter.append(FilterInput())
    list_filter.append(FilterConstruct())
    list_filter.append(FilterHuffman())
    list_filter.append(FilterEncode())
    list_filter.append(FilterWrite())
    list_filter.append(FilterDictionary())
    list_filter.append(FilterDecode())

    list_pipe[0].next_filter = list_filter[0]
    list_pipe[1].prev_filter = list_filter[0]
    list_pipe[1].next_filter = list_filter[1]
    list_pipe[2].prev_filter = list_filter[1]
    list_pipe[2].next_filter = list_filter[2]
    list_pipe[3].prev_filter = list_filter[2]
    list_pipe[3].next_filter = list_filter[3]
    list_pipe[4].prev_filter = list_filter[3]
    list_pipe[4].next_filter = list_filter[4]
    list_pipe[5].prev_filter = list_filter[0]
    list_pipe[5].next_filter = list_filter[5]
    list_pipe[6].prev_filter = list_filter[5]
    list_pipe[6].next_filter = list_filter[6]
    list_pipe[7].prev_filter = list_filter[6]
    list_pipe[7].next_filter = list_filter[4]

    for i in list_pipe:
        i.setDaemon(True)
        i.start()

    return list_pipe, list_filter


def main():
    global exit_flag
    list_pipe, list_filter = construct_pipeline()
    print("Filename with format:")
    name_file = sys.stdin.readline().strip()
    while name_file != "EXIT":
        temp_list = name_file.split("||")
        for i in temp_list:
            temp_pos = i.rfind("/")
            filename = i[:temp_pos]
            if os.path.isfile(filename):
                if "/e" in i:
                    mode = "encode"
                else:
                    mode = "decode"
                list_pipe[0].add_storage([mode, filename])
            else:
                print "File not found"

        name_file = sys.stdin.readline().strip()

    exit_flag = True
    for i in range(0, 5):
        list_pipe[i].join()

    for i in list_filter:
        del i

main()
