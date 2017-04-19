import threading
import sys
import os.path
# import binascii
# from bitarray import bitarray
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
        self.__filter_input = None


class FilterInput(Filter):
    __filename = ""
    __plain_data = ""

    def insert_input(self, filter_input):
        self.__filename = filter_input

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
    def insert_input(self, filter_input):
        self.__filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        self.busy = True
        print "--ID " + self.processing + " Running FilterConstruct"
        input_text = self.__filter_input[0]
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
        self.output = [temp_result, temp_count2, input_text, temp_list2, self.__filter_input[1]]
        self.busy = False


class FilterHuffman(Filter):
    __dict_converted = {}

    def insert_input(self, filter_input):
        self.__filter_input = filter_input[1]
        self.processing = filter_input[0]

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
        if self.__filter_input:
            self.busy = True
            print "--ID " + self.processing + " Running FilterHuffman"
            list_alphabet = self.__filter_input[0]
            list_count = self.__filter_input[1]
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
            self.output = [result, self.__filter_input[2], self.__filter_input[3], self.__filter_input[4]]
            self.busy = False


class FilterEncode(Filter):
    def insert_input(self, filter_input):
        self.__filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        if self.__filter_input:
            self.busy = True
            print "--ID " + self.processing + " Running FilterEncode"
            result = ""
            dict_converted = self.__filter_input[0]
            plain_text = self.__filter_input[1]
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

            self.output = [len(plain_text), dict_converted, final_result,
                           self.__filter_input[2], self.__filter_input[3]]
            self.busy = False


class FilterWrite(Filter):
    def insert_input(self, filter_input):
        self.__filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        if self.__filter_input:
            length = self.__filter_input[0]
            dict_converted = self.__filter_input[1]
            converted_data = self.__filter_input[2]
            rank_alphabet = self.__filter_input[3]
            filename = self.__filter_input[4]
            output_file = filename + ".d2f"
            temp = False
            lock_file = None

            self.busy = True
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
                result += str(length) + "-" + str(temp) + "" + "-|=" + i
                if rank_alphabet.index(i) < len(rank_alphabet) - 1:
                    result += "/|"

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
            self.busy = False


class FilterDictionary(Filter):
    def insert_input(self, filter_input):
        self.__filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        self.busy = True
        filename = self.__filter_input[1]
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
        temp_list = temp.split('-|=')
        dict_binary = {}

        for i in range(0, len(temp_list)):
            temp = temp_list[i]
            temp_list2 = temp.split('/|')
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

            dict_binary[temp_list2[1]] = bin_val
        self.output = [filename, size, dict_binary, self.__filter_input[0]]


class Pipe(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.storage_data = []
        self.in_work = ""
        self.prev_filter = None
        self.next_filter = None
        self.running = True

    def stop(self):
        self._stop.set()
        self.running = False

    def add_storage(self, data):
        self.storage_data.append(data)

    def check_prev(self):
        if not self.prev_filter.busy and self.prev_filter.output:
            temp_value = self.prev_filter.output
            self.prev_filter.output = None
            temp_key = self.prev_filter.processing
            self.in_work = temp_key
            self.storage_data.append([temp_key, temp_value])

    def check_next(self):
        size = len(self.storage_data)
        if not self.next_filter.busy and size > 0:
            temp_data = self.storage_data.pop(0)
            self.next_filter.insert_input(temp_data)
            self.next_filter.run()

    def run(self):
        while not exit_flag:
            if self.prev_filter:
                self.check_prev()
            if self.next_filter:
                self.check_next()

        self.running = False


def main():
    global exit_flag
    list_pipe = []
    head_pipe = Pipe()
    list_pipe.append(head_pipe)
    for i in range(0, 4):
        list_pipe.append(Pipe())

    head_filter = FilterInput()
    second_filter = FilterConstruct()
    third_filter = FilterHuffman()
    fourth_filter = FilterEncode()
    fifth_filter = FilterWrite()
    head_pipe.next_filter = head_filter
    list_pipe[1].prev_filter = head_filter
    list_pipe[1].next_filter = second_filter
    list_pipe[2].prev_filter = second_filter
    list_pipe[2].next_filter = third_filter
    list_pipe[3].prev_filter = third_filter
    list_pipe[3].next_filter = fourth_filter
    list_pipe[4].prev_filter = fourth_filter
    list_pipe[4].next_filter = fifth_filter

    for i in list_pipe:
        i.setDaemon(True)
        i.start()

    print("Filename with format:")
    # sys.stdout.write(">> ")
    name_file = sys.stdin.readline().strip()
    while name_file != "EXIT":
        if os.path.isfile(name_file):
            head_pipe.add_storage(name_file)
        else:
            print "File not found"

        name_file = sys.stdin.readline().strip()

    exit_flag = True
    for i in range(0, 5):
        list_pipe[i].join()


main()
