from collections import defaultdict
from typing import Tuple

import numpy as np

from datatrove.data import Document
from datatrove.io import cached_asset_path_or_download
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.text import SPLIT_TEXT_DOCUMENTS, split_into_parts
from datatrove.utils.typeshelper import Languages


class FastTextClassifierFilter(BaseFilter):
    """
    Only keeps documents that have
    - AT LEAST ONE of the labels in `keep_labels` with a score above the configured threshold, or
    - NONE of the labels in `remove_labels` with a score above the configured threshold.

    You can only supply one of these, to avoid conflicts. Use multiple filters if you need to. If you supply
    neither, the block will simply annotate each document with the labels (set `save_labels_in_metadata=True`)

    Example:
        for `keep_labels=[("math", 0.9)]` will only keep samples with a score on __label__math of at least 0.9   # 只保留__label__math得分至少为0.9的样本
        for `remove_labels=[("math", 0.9)]` will remove samples with a score on __label__math of at least 0.9    # 移除__label__math得分至少为0.9的样本

    Info to train your own classifier: https://fasttext.cc/docs/en/supervised-tutorial.html

    Args:
        model_url: url to download the model from or local path    # 模型文件的URL或本地路径
        keep_labels: tuple of (label name without "__label__", min score) (or list of such tuples)    # 需要保留的标签及其最小得分
        remove_labels: tuple of (label name without "__label__", min score) (or list of such tuples)    # 需要移除的标签及其最小得分
        save_labels_in_metadata: whether to save all the label scores in the document metadata    # 是否将所有标签得分保存到文档元数据中
        newline_replacement: str to replace \n with before predicting scores    # 预测前替换换行符的替换字符
        filter_mode: predict and filter on DOCUMENT, PARAGRAPH or SENTENCE level    # 预测和过滤的粒度（文档、段落或句子）
        # language: language code for text tokenization (default: english)
        exclusion_writer:    # 写入被过滤的数据
    """

    name = "🤖 fastText"
    _requires_dependencies = [("fasttext", "fasttext-wheel"), "fasteners"]

    def __init__(
        self,
        model_url: str,
        keep_labels: Tuple[str, float] | list[Tuple[str, float]] | None = None,
        remove_labels: Tuple[str, float] | list[Tuple[str, float]] | None = None,
        save_labels_in_metadata: bool = True,
        exclusion_writer: DiskWriter | None = None,
        newline_replacement="",    # 预测前替换换行符的替换字符
        filter_mode: str = SPLIT_TEXT_DOCUMENTS,    # 预测和过滤的粒度（文档、段落或句子）
        language: str = Languages.chinese,  # 语言设置
        debug: bool = False,  # 添加调试模式
    ):
        super().__init__(exclusion_writer)
        self.model_url = model_url
        self.keep_labels = keep_labels
        self.remove_labels = remove_labels
        self.filter_mode = filter_mode
        self.language = language  # 添加这行
        if keep_labels and remove_labels:
            raise ValueError("You can only supply one of `keep_labels` or `remove_labels`.")
        self.newline_replacement = newline_replacement
        if keep_labels and isinstance(keep_labels[0], str):
            self.keep_labels = [keep_labels]
        if remove_labels and isinstance(remove_labels[0], str):
            self.remove_labels = [remove_labels]
        self.save_labels_in_metadata = save_labels_in_metadata
        self._model = None
        self.debug = debug

    @property
    def model(self):
        if self._model is None:
            from fasttext.FastText import _FastText

            model_file = cached_asset_path_or_download(  # 缓存模型文件
                self.model_url, namespace="filters", subfolder="fasttext", desc="fast-text model"
            )
            self._model = _FastText(model_file)
            # check label values
            available_labels = [x.removeprefix("__label__") for x in self._model.labels]
            for label, _ in self.keep_labels or [] + self.remove_labels or []:
                if label not in available_labels:
                    raise ValueError(
                        f"Label '{label}' passed as keep_labels or remove_labels is not available in this "
                        f"FastText model. Available labels: {available_labels}"
                    )
        return self._model

    def filter_my(self, doc: Document) -> bool:    # 过滤文档
        def check_label_scores(unit_scores):    # 检查标签得分
            if self.keep_labels:
                return any(
                    unit_scores.get(f"__label__{label}", -9e9) >= min_score for label, min_score in self.keep_labels
                )
            else:
                return not self.remove_labels or not any(
                    unit_scores.get(f"__label__{label}", -9e9) >= min_score for label, min_score in self.remove_labels
                )

        # 使用与训练时相同的处理方式
        # if self.language == "zh":
        import jieba
        import re
        original_text = doc.text
        processed_text = re.sub(r'\s+', ' ', ' '.join(jieba.lcut(doc.text)))
        # else:
        #     processed_text = doc.text.strip().replace("\n", self.newline_replacement)

        # 预测
        labels, scores = self.model.predict(processed_text, k=-1, threshold=0)
        
        # 检查标签得分
        unit_scores = dict(zip(labels, scores))
        should_keep = check_label_scores(unit_scores)
        
        # 更新文档
        if should_keep:
            doc.text = original_text
            if self.save_labels_in_metadata:
                doc.metadata.update({label: score for label, score in zip(labels, scores)})
            return True
        else:
            return False
        
    
    def filter(self, doc: Document) -> bool:
        import jieba
        import re
        def check_label_scores(unit_scores):
            if self.keep_labels:
                return any(
                    unit_scores.get(f"__label__{label}", -9e9) >= min_score for label, min_score in self.keep_labels
                )
            else:
                return not self.remove_labels or not any(
                    unit_scores.get(f"__label__{label}", -9e9) >= min_score for label, min_score in self.remove_labels
                )

        units = split_into_parts(doc.text, mode=self.filter_mode)
        kept_spans = []
        label_scores = defaultdict(list)
        for unit in units:
            # labels, scores = self.model.predict(unit.strip().replace("\n", self.newline_replacement), k=-1)
            processed_text = re.sub(r'\s+', ' ', ' '.join(jieba.lcut(unit)))
            labels, scores = self.model.predict(processed_text, k=-1, threshold=0)
            if self.save_labels_in_metadata:
                for label, score in zip(labels, scores):
                    label_scores[label].append(score)
            if check_label_scores(dict(zip(labels, scores))):
                kept_spans.append(unit)
                self.stat_update("kept_span")
            else:
                self.stat_update("removed_span")
        doc.text = "".join(kept_spans)
        if self.save_labels_in_metadata:
            doc.metadata.update({label: np.mean(scores).item() for label, scores in label_scores.items()})
        return not not doc.text.strip()

    
