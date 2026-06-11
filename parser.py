import re
import html
from typing import Optional, List, Union
from bs4 import BeautifulSoup, Tag

JSONValue = Union[str, int, float, bool, None]

class Parser:

    HTML_FIELD_ATTRS = (
        "data-e2e",
        "data-field",
        "data-testid",
        "data-name",
        "data-key",
        "name",
        "id",
    )

    LITERAL_RE = r"([A-Za-z0-9_+\-.eE]+)"

    def __init__(self, html_blob: str, parser: str = "html.parser"):
        self.raw = html_blob
        self.soup = BeautifulSoup(html_blob, parser)

    def find(self, field_name: str, strategy: str = "auto", html_attr: Optional[str] = None) -> Optional[JSONValue]:
        if strategy in ("auto", "json"):
            val = self.extract_json(field_name)
            if val is not None:
                return val

        if strategy in ("auto", "html"):
            val = self.extract_html(field_name, html_attr=html_attr)
            if val is not None:
                return val

        return None

    def find_all(self, field_name: str, strategy: str = "auto", html_attr: Optional[str] = None) -> List[JSONValue]:
        results = []

        if strategy in ("auto", "json"):
            results.extend(self.extract_all_json(field_name))

        if strategy in ("auto", "html"):
            results.extend(self.extract_all_html(field_name, html_attr=html_attr))

        return results

    def extract_json(self, field_name: str) -> Optional[JSONValue]:
        return next(self.iter_json_values(field_name), None)

    def extract_all_json(self, field_name: str) -> List[JSONValue]:
        return list(self.iter_json_values(field_name))

    def json_matchers(self, field_name: str):
        esc = re.escape(field_name)

        return (
            # "field": "value"
            (
                re.compile(
                    r'"{}"\s*:\s*("(?:\\.|[^"\\])*"|[A-Za-z0-9_+\-.eE]+)'.format(esc)
                ),
                None,
            ),
            # 'field': 'value' or 'field': "value"
            (
                re.compile(
                    r"'{}'\s*:\s*(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[A-Za-z0-9_+\-.eE]+)".format(esc)
                ),
                None,
            ),
            # \"field\": \"value\"
            (
                re.compile(r'\\"{}\\"\s*:\s*'.format(esc)),
                '\\"',
            ),
            # \'field\': \'value\'
            (
                re.compile(r"\\'{}\\'\s*:\s*".format(esc)),
                "\\'",
            ),
        )

    def iter_json_values(self, field_name: str):
        for pattern, escaped_end_seq in self.json_matchers(field_name):
            for match in pattern.finditer(self.raw):
                if escaped_end_seq is None:
                    yield self.parse_value(match.group(1))
                else:
                    yield self.parse_escaped_or_literal(match.end(), escaped_end_seq)

    def parse_escaped_or_literal(self, start: int, end_seq: str) -> JSONValue:
        val = self.scan_escaped_string(start, end_seq=end_seq)
        if val is not None:
            return val

        match = re.match(self.LITERAL_RE, self.raw[start:])
        if match:
            return self.parse_literal(match.group(1))

        return None

    def scan_escaped_string(self, start: int, end_seq: str = '\\"') -> Optional[str]:
        if start >= len(self.raw):
            return None

        quote = '"' if end_seq == '\\"' else "'"

        if not (
            self.raw[start] == "\\"
            and start + 1 < len(self.raw)
            and self.raw[start + 1] == quote
        ):
            return None

        i = start + 2
        chars = []

        while i < len(self.raw):
            if self.raw[i] == "\\" and i + 1 < len(self.raw):
                if self.raw[i:i + 2] == end_seq:
                    return "".join(chars)

                chars.append(self.decode_escape(self.raw[i + 1]))
                i += 2
            else:
                chars.append(self.raw[i])
                i += 1

        return None

    @staticmethod
    def decode_escape(char: str) -> str:
        return {
            "n": "\n",
            "t": "\t",
            "r": "\r",
            "\\": "\\",
            "/": "/",
        }.get(char, char)

    @staticmethod
    def parse_value(raw: str) -> Union[str, int, float, bool, None]:
        raw = raw.strip()

        if raw.startswith('"') and raw.endswith('"'):
            inner = raw[1:-1]
            return (
                inner
                .replace('\\"', '"')
                .replace('\\\\', '\\')
                .replace('\\n', '\n')
                .replace('\\t', '\t')
                .replace('\\/', '/')
            )

        if raw.startswith("'") and raw.endswith("'"):
            inner = raw[1:-1]
            return (
                inner
                .replace("\\'", "'")
                .replace('\\\\', '\\')
                .replace('\\n', '\n')
                .replace('\\t', '\t')
            )

        return Parser.parse_literal(raw)

    @staticmethod
    def parse_literal(raw: str) -> Union[str, int, float, bool, None]:
        raw = raw.strip()
        lower = raw.lower()

        if lower == "true":
            return True
        if lower == "false":
            return False
        if lower == "null":
            return None

        if re.fullmatch(r'[+-]?\d+', raw):
            return int(raw)

        if re.fullmatch(r'[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?', raw):
            return float(raw)

        return raw

    def extract_html(self, field_name: str, html_attr: Optional[str] = None) -> Optional[str]:
        return next(self.iter_html_values(field_name, html_attr), None)

    def extract_all_html(self, field_name: str, html_attr: Optional[str] = None) -> List[str]:
        return list(self.iter_html_values(field_name, html_attr))

    def iter_html_values(self, field_name: str, html_attr: Optional[str] = None):
        seen = set()

        for elem, icon_context in self.iter_html_matches(field_name):
            elem_id = id(elem)
            if elem_id in seen:
                continue

            seen.add(elem_id)

            yield self.render_html_result(
                elem,
                html_attr=html_attr,
                icon_context=icon_context,
            )

    def iter_html_matches(self, field_name: str):
        for attr in self.HTML_FIELD_ATTRS:
            for elem in self.soup.find_all(attrs={attr: field_name}):
                yield elem, False

        for elem in self.find_all_by_class_tokens(field_name):
            yield elem, True

        for elem in self.soup.find_all(
            attrs={"class": lambda x: x and field_name in x}
        ):
            yield elem, False

    @staticmethod
    def class_tokens(field_name: str) -> List[str]:
        normalized = field_name.strip().replace(".", " ")
        return [token for token in normalized.split() if token]

    def find_by_class_tokens(self, field_name: str) -> Optional[Tag]:
        return next(iter(self.find_all_by_class_tokens(field_name)), None)

    def find_all_by_class_tokens(self, field_name: str) -> List[Tag]:
        tokens = self.class_tokens(field_name)
        if not tokens:
            return []

        return list(self.soup.find_all(class_=self.class_matcher(tokens)))

    @staticmethod
    def class_matcher(tokens: List[str]):
        token_set = set(tokens)

        def matches(classes) -> bool:
            if not classes:
                return False

            classes = classes if isinstance(classes, list) else str(classes).split()
            return token_set.issubset(classes)

        return matches

    @staticmethod
    def render_text_from_icon_context(elem: Tag) -> str:
        """
        If the matched element is an icon/marker with no own text, return
        the nearest parent text. Supports:
        <p><i class="fa fa-user"></i> Age 33</p>
        """
        own_text = Parser.render_text(elem)
        if own_text:
            return own_text

        parent = elem.parent
        if isinstance(parent, Tag):
            return Parser.render_text(parent)

        return ""

    @staticmethod
    def render_html_result(elem: Tag, html_attr: Optional[str] = None, icon_context: bool = False) -> Optional[str]:
        if html_attr:
            target = elem

            if icon_context and html_attr not in target.attrs and isinstance(elem.parent, Tag):
                target = elem.parent

            val = target.get(html_attr)
            if val is None:
                return None

            if isinstance(val, list):
                return " ".join(map(str, val))

            return html.unescape(str(val))

        if icon_context:
            return Parser.render_text_from_icon_context(elem)

        return Parser.render_text(elem)

    @staticmethod
    def render_text(elem: Tag) -> str:
        if elem.name in ("input", "textarea", "select"):
            val = elem.get("value")
            if val is not None:
                return html.unescape(str(val))

        return html.unescape(elem.get_text(separator=" ", strip=True))
