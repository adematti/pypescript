"""Generate **pypescript** modules rst documentation from module description files."""

import re

from . import utils


def generate_rst_doc_table(rows, max_line_len=80):
    """
    Generate rst table from module documentation ``rows``.

    Parameters
    ----------
    rows : dict
        Documentation dictionary.

    max_line_len : int, default=80
        Max table line length. Default length chosen to match the width of ``sphinx_rtd_theme``.
        Note that only the width of the last column of the table will be impacted by this parameter
        (hence the minimum with of the table is set to the size of all the columns except the last one).

    Returns
    -------
    rst : string
        rst-formatted table.

    Note
    ----
    This function has not been thoroughly tested.
    """
    rows = dict(rows)
    # first turn description into a list of lines
    def lines_callback(row):
        lines = []
        if isinstance(row,dict) and row:
            for key,value in row.items():
                lines_ = lines_callback(value)
                lines.append([key] + lines_[0])
                lines += [[''] + li for li in lines_[1:]]
        elif isinstance(row,list):
            for value in row:
                lines.append(['- {}'.format(value)])
            if not row:
                lines.append([''])
        else:
            row = str(row)
            if row.endswith('\n'): row = row[:-1]
            lines.append([row])
        return lines

    rows = lines_callback(rows)
    lines = []
    add_horizontal_line = []
    for line in rows:
        lines.append(line)
        add_horizontal_line.append(True)
        # line length = length of all words + ' | ' * (number of columns - 1) + left + right
        line_len = sum([len(el) for el in line]) + 3*(len(line) - 1) + 4
        # if line length larger than maximum allowed line length, split last column
        if line_len > max_line_len:
            phrase_len = len(line[-1]) + max_line_len - line_len # maximum allowed width for last column
            #words = line[-1].split(' ')
            #words = re.split(''' (?=(?:[^'"`]|`[^`]*`|'[^']*'|"[^"]*")*$)''',line[-1]) # does not split everything that is between quotes "" '' ``
            words = re.split(''' (?=(?:[^`]|`[^`]*`)*$)''',line[-1]) # does not split everything that is between quotes ``
            phrases = [words[0]]
            for word in words[1:]: # stack words of last column phrase till they reach last column width
                if len(word) + 1 + len(phrases[-1]) < phrase_len:
                    phrases[-1] += ' ' + word
                else:
                    phrases.append(word)
            #print(phrases)
            # replace last column phrase by cut phrase
            lines[-1][-1] = '| ' + phrases[0]
            for phrase in phrases[1:]: # add new lines
                new_line = ['']*(len(line)-1) + ['| ' + phrase]
                lines.append(new_line)
                add_horizontal_line.append(False)

    # get divisions = column widths
    divs,ncols = [],[]
    for line in lines:
      divs.append([len(el) for el in line])
      ncols.append(len(line))
    max_ncols = max(ncols)

    for icol in range(max_ncols):
        max_len = 0
        last_idiv = 0
        for idiv,div in enumerate(divs): # determine largest width of icolth column
            if len(div) > icol + 1: # icol + 1 because do not touch last column of each line
                max_len = max(max_len,div[icol])
            else: # if less columns, reinitialise max length
                for div in divs[last_idiv:idiv]:
                    div[icol] = max_len
                max_len = 0
                last_idiv = idiv + 1
        for div in divs[last_idiv:]:
            div[icol] = max_len

    div_len = []
    for div in divs:
        # column division length = total length + ' + ' * (number of columns - 1)
        div_len.append(sum(div) + 3*(len(div) - 1))
    total_len = max(div_len)
    for div,ln in zip(divs,div_len):
        div[-1] += total_len - ln

    def get_horizontal_line(div,sep='-',nskip=0):
        toret = ''
        for id,d in enumerate(div):
            if id < nskip:
                toret += '|' + ' '*(2 + d)
            else:
                toret += '+' + sep*(2 + d)
        toret += '+\n'
        return toret

    def get_text_line(line,div,nskip=0):
        toret = ''
        for il,(el,d) in enumerate(zip(line,div)):
            if il < nskip:
                toret += '| ' + ' '*(d + 1)
            else:
                toret += '| ' + el + ' '*(d - len(el) + 1)
        toret += '|\n'
        return toret

    rst = ''
    for iline,(line,div) in enumerate(zip(lines,divs)):
        sep = '=' if iline == 1 else '-'
        nskip = 0
        for il,el in enumerate(line):
            if el:
                nskip = il
                break
        if nskip == len(line) - 1:
            # this is the case where last column was split into several lines
            nskiph = nskip + 1
        else:
            nskiph = nskip
        #nskip = 0
        if add_horizontal_line[iline]:
            if iline > 0 and len(divs[iline-1]) > len(div):
                # previous line has more columns
                rst += get_horizontal_line(divs[iline-1],sep=sep,nskip=nskip)
            else:
                rst += get_horizontal_line(div,sep=sep,nskip=nskiph)
        rst += get_text_line(line,div,nskip=nskip)
    rst += get_horizontal_line(divs[-1],sep='-')
    return rst


def generate_pype_modules_rst_doc(section_underline='-',max_line_len=80, **kwargs):
    """
    Generate rst tables for several modules of the **pypescript** library.

    Parameters
    ----------
    kwargs : dict
        Arguments for :func:`utils.walk_pype_modules`.

    Returns
    -------
    rst : string
        rst-formatted list of tables.
    """
    rst = ''
    current_section = ''
    for module_dir,full_name,description_file,description in utils.walk_pype_modules(**kwargs):
        section = full_name.split('.')[0]
        if section and section != current_section:
            rst += section + '\n'
            rst += section_underline*len(section) + '\n'
            current_section = section
        rst += 'Description of module :mod:`{}`:\n\n'.format(full_name)
        rst += generate_rst_doc_table(description,max_line_len=max_line_len) + '\n\n'
    return rst


def write_pype_modules_rst_doc(filename, header='', title='Modules', title_underline='=', **kwargs):
    """
    Generate rst file with tables for several modules of the **pypescript** library.

    Parameters
    ----------
    filename : string
        Where to save rst file.

    header : string, default=''
        To add on top of the rst file.

    title : string, default='Modules'
        Title.

    underline : string, default='='
        Title underline.

    kwargs : dict
        Arguments for :func:`generate_pype_modules_rst_doc`.

    Returns
    -------
    rst : string
        rst-formatted list of tables.
    """
    utils.mkdir(filename)
    with open(filename,'w') as file:
        file.write(header)
        if header: file.write('\n')
        file.write(title + '\n')
        file.write(title_underline*len(title) + '\n\n')
        file.write(generate_pype_modules_rst_doc(**kwargs))
