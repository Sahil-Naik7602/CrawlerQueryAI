import heapq
from collections import defaultdict

from HuffmanNode import HuffmanNode


class HuffmanTextCompressor:
    def __init__(self):
        self.codes = {}
        self.reverse_mapping = {}

    def build_frequency_dict(self, text):
        frequency = defaultdict(int)
        for char in text:
            frequency[char] += 1
        return frequency

    def build_heap(self, frequency):
        heap = []
        for key in frequency:
            node = HuffmanNode(key, frequency[key])
            heapq.heappush(heap, node)
        return heap

    def merge_nodes(self, heap):
        while len(heap) > 1:
            node1 = heapq.heappop(heap)
            node2 = heapq.heappop(heap)
            merged = HuffmanNode(None, node1.freq + node2.freq)
            merged.left = node1
            merged.right = node2
            heapq.heappush(heap, merged)
        return heap[0]

    def build_codes_helper(self, node, current_code):
        if node is None:
            return
        if node.char is not None:
            self.codes[node.char] = current_code
            self.reverse_mapping[current_code] = node.char
        self.build_codes_helper(node.left, current_code + "0")
        self.build_codes_helper(node.right, current_code + "1")

    def build_codes(self, root):
        self.build_codes_helper(root, "")

    def get_encoded_text(self, text):
        encoded_text = "".join(self.codes[char] for char in text)
        return encoded_text

    def pad_encoded_text(self, encoded_text):
        extra_padding = 8 - len(encoded_text) % 8
        encoded_text += "0" * extra_padding
        padded_info = "{0:08b}".format(extra_padding)
        return padded_info + encoded_text

    def compress(self, text):
        frequency = self.build_frequency_dict(text)
        heap = self.build_heap(frequency)
        root = self.merge_nodes(heap)
        self.build_codes(root)
        encoded_text = self.get_encoded_text(text)
        padded_text = self.pad_encoded_text(encoded_text)
        return padded_text

    def remove_padding(self, padded_text):
        padded_info = padded_text[:8]
        extra_padding = int(padded_info, 2)
        padded_text = padded_text[8:]
        encoded_text = padded_text[:-extra_padding]
        return encoded_text

    def decode_text(self, encoded_text):
        current_code = ""
        decoded_text = ""
        for bit in encoded_text:
            current_code += bit
            if current_code in self.reverse_mapping:
                decoded_text += self.reverse_mapping[current_code]
                current_code = ""
        return decoded_text

    def decompress(self, padded_text):
        encoded_text = self.remove_padding(padded_text)
        decoded_text = self.decode_text(encoded_text)
        return decoded_text