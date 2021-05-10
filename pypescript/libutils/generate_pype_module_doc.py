"""Generate **pypescript** modules rst documentation from module description files."""

from . import utils


def generate_doc_rst_table(rows, max_line_len=80):
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

    def lines_callback(row,lines=[]):
        if isinstance(row,dict):
            for key,value in row.items():
                lines_ = []
                lines_callback(value,lines_)
                lines.append([key] + lines_[0])
                lines += [[''] + li for li in lines_[1:]]
        elif isinstance(row,list):
            for el in row:
                lines.append(['- {}'.format(el)])
            if not row:
                lines.append([''])
        else:
            lines.append([str(row)])

    lines = []
    lines_callback(rows,lines)

    rows = lines
    lines = []
    for line in rows:
        lines.append(line)
        line_len = sum([len(el) for el in line]) + 3*(len(line) - 1) + 4
        if line_len > max_line_len:
            phrase_len = len(line[-1]) + max_line_len - line_len
            words = line[-1].split(' ')
            phrases = [words[0]]
            for word in words[1:]:
                if len(word) + 1 + len(phrases[-1]) < phrase_len:
                    phrases[-1] += ' ' + word
                else:
                    phrases.append(word)
            #print(phrases)
            lines[-1][-1] = phrases[0]
            for phrase in phrases[1:]:
                new_line = ['']*(len(line)-1) + [phrase]
                lines.append(new_line)

    divs,ncols = [],[]
    for line in lines:
      divs.append([len(el) for el in line])
      ncols.append(len(line))
    max_ncols = max(ncols)
    for icol in range(max_ncols):
        max_len = 0
        for div in divs:
            if len(div) > icol + 1:
                max_len = max(max_len,div[icol])
        for div in divs:
            if len(div) > icol + 1:
                div[icol] = max_len

    div_len = []
    for div in divs:
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
            nskiph = nskip + 1
        else:
            nskiph = nskip
        #nskip = 0
        if iline > 0 and len(divs[iline-1]) > len(div):
            rst += get_horizontal_line(divs[iline-1],sep=sep,nskip=nskip)
        else:
            rst += get_horizontal_line(div,sep=sep,nskip=nskiph)
        rst += get_text_line(line,div,nskip=nskip)
    rst += get_horizontal_line(divs[-1],sep='-')
    return rst


def generate_pype_modules_rst_doc(**kwargs):
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
    for module_dir,full_name,description_file,description in utils.walk_pype_modules(**kwargs):
        rst += 'Description of module :mod:`{}`:\n\n'.format(full_name)
        rst += generate_doc_rst_table(description) + '\n\n'
    return rst


def write_pype_modules_rst_doc(filename, header='', title='Modules', underline='=', **kwargs):
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
    with open(filename, 'w') as file:
        file.write(header)
        if header: file.write('\n')
        file.write(title+'\n')
        file.write(underline*len(title)+'\n\n')
        file.write(generate_pype_modules_rst_doc(**kwargs))
