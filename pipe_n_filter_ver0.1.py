import threading
import sys
import os.path
from copy import copy
from lockfile import FileLock
from datetime import datetime
import time


class FilterInput(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.filename = ""
        self.plain_data = ""
        self.busy = False
        self.output = None
        self._stop = threading.Event()
        self.processing = ""

    def insert_input(self, filter_input):
        self.filename = filter_input

    def run(self):
        self.busy = True
        key = datetime.now().strftime('%Y%m%d%H%M%S')
        self.processing = key
        print "--ID " + self.processing + " Running FilterInput "
        self.output = None

        lock_file = FileLock(self.filename)
        status = lock_file.is_locked()

        while status:
            status = lock_file.is_locked()
        if os.path.isfile(self.filename):
            file_open = open(self.filename, "rb")
            self.plain_data = file_open.read()
            file_open.close()
            self.output = [self.plain_data, self.filename]
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


class FilterConstruct(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.busy = False
        self.output = None
        self.processing = ""
        self.filter_input = None

    def insert_input(self, filter_input):
        self.filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        if self.filter_input:
            print "--ID " + self.processing + " Running FilterConstruct"
            self.busy = True
            self.output = None
            input_text = self.filter_input
            temp_count = []
            temp_result = []
            temp_list = list(set(input_text))
            for i in range(0, len(temp_list)):
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
            temp_count2.sort(reverse=True)
            self.output = [temp_result, temp_count2, input_text, temp_list2, self.filter_input[1]]
            self.busy = False


class FilterHuffman(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.busy = False
        self.dict_converted = {}
        self.output = None
        self.processing = ""
        self.filter_input = None

    def insert_input(self, filter_input):
        self.filter_input = filter_input[1]
        self.processing = filter_input[0]

    def get_result(self, start, head):
        if head.status == 0:
            temp_result = start
            temp_alphabet = head.alphabet
            self.dict_converted[temp_alphabet] = temp_result
            return
        else:
            start1 = start + "0"
            start2 = start + "1"
            self.get_result(start1, head.left)
            self.get_result(start2, head.right)
            return

    def run(self):
        if self.filter_input:
            self.busy = True
            print "--ID " + self.processing + " Running FilterHuffman"
            list_alphabet = self.filter_input[0]
            list_count = self.filter_input[1]
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
                list_count.sort(reverse=True)
                temp_index = list_count.index(temp_count)
                list_alphabet = list_alphabet[:temp_index] + [temp_parent] + list_alphabet[temp_index:]
                alpha_count -= 1

            self.get_result("", list_alphabet.pop(0))
            result = copy(self.dict_converted)
            self.dict_converted.clear()
            self.output = [result, self.filter_input[2], self.filter_input[3], self.filter_input[4]]
            self.busy = False


class FilterEncode(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.busy = False
        self.output = None
        self.processing = ""
        self.filter_input = None

    def insert_input(self, filter_input):
        self.filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        if self.filter_input:
            self.busy = True
            print "--ID " + self.processing + " Running FilterEncode"
            result = ""
            dict_converted = self.filter_input[0]
            plain_text = self.filter_input[1]
            for i in plain_text:
                result += dict_converted[i]

            count_zero = len(result) % 8
            result += '0' * count_zero
            final_result = ""
            it = 0
            size = len(result)
            while it < size:
                temp_int = int(result[it:it+8], 2)
                final_result += chr(temp_int)
                it += 8

            self.output = [len(plain_text), dict_converted, final_result, self.filter_input[2], self.filter_input[3]]
            self.busy = False


class FilterWrite(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.busy = False
        self.processing = ""
        self.filter_input = None

    def insert_input(self, filter_input):
        self.filter_input = filter_input[1]
        self.processing = filter_input[0]

    def run(self):
        if self.filter_input:
            length = self.filter_input[0]
            dict_converted = self.filter_input[1]
            converted_data = self.filter_input[2]
            rank_alphabet = self.filter_input[3]
            filename = self.filter_input[4]
            output_file = filename + "d2f"
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
                result += chr(int(dict_converted[i], 2))
                if rank_alphabet.index(i) < len(rank_alphabet) - 1:
                    result += "_|_"

            output_file = filename + "d2c"
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


class Pipe(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.storage_data = []
        self.in_work = ""
        self.prev_filter = None
        self.next_filter = None

    def add_storage(self, data):
        self.storage_data.append(data)

    def check_prev(self):
        while True:
            if not self.prev_filter.busy:
                temp_value = self.prev_filter.output
                temp_key = self.prev_filter.processing
                self.in_work = temp_key
                self.storage_data.append([temp_key, temp_value])

    def check_next(self):
        while True:
            if not self.next_filter.busy and len(self.storage_data) > 0:
                temp_data = self.storage_data.pop(0)
                self.next_filter.insert_input(temp_data)
                self.next_filter.start()

    def run(self):
        thread_list = []
        if self.prev_filter:
            temp = threading.Thread(self.check_prev())
            thread_list.append(temp)
            temp.start()
        if self.next_filter:
            temp2 = threading.Thread(self.check_next())
            thread_list.append(temp2)
            temp2.start()


def main():
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
        i.start()

    print("Filename with format:")
    sys.stdout.write(">> ")
    name_file = sys.stdin.readline().strip()
    while name_file != "EXIT":
        if os.path.isfile(name_file):
            head_pipe.add_storage(name_file)
        else:
            print "File not found"

        print("Filename with format:")
        sys.stdout.write(">> ")
        name_file = sys.stdin.readline()


t_main = threading.Thread(main())
t_main.start()
