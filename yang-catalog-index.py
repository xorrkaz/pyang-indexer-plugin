
from pyang import plugin, statements
import json
import optparse
import re

_yang_catalog_index_fd = None


def pyang_plugin_init():
    plugin.register_plugin(IndexerPlugin())


class IndexerPlugin(plugin.PyangPlugin):

    def add_output_format(self, fmts):
        self.multiple_modules = True
        fmts['yang-catalog-index'] = self

    def add_opts(self, optparser):
        optlist = [
            optparse.make_option("--yang-index-no-schema",
                                 dest="yang_index_no_schema",
                                 action="store_true",
                                 help="""Do not include SQLite schema in output"""),
            optparse.make_option("--yang-index-schema-only",
                                 dest="yang_index_schema_only",
                                 action="store_true",
                                 help="""Only include the SQLite schema in output"""),
            optparse.make_option("--yang-index-make-module-table",
                                 dest="yang_index_make_module_table",
                                 action="store_true",
                                 help="""Generate a modules table that includes various aspects about the modules themselves""")
        ]

        g = optparser.add_option_group("YANG Catalog Index specific options")
        g.add_options(optlist)

    def setup_fmt(self, ctx):
        ctx.implicit_errors = False

    def emit(self, ctx, modules, fd):
        global _yang_catalog_index_fd

        _yang_catalog_index_fd = fd
        emit_index(ctx, modules, fd)


def emit_index(ctx, modules, fd):
    if not ctx.opts.yang_index_no_schema:
        fd.write(
            "create table yindex(module, revision, path, statement, argument, description, properties);\n")
        if ctx.opts.yang_index_make_module_table:
            fd.write(
                "create table modules(module, revision, yang_version, belongs_to, namespace, prefix, organization, maturity, compile_status, document, file_path);\n")
    if not ctx.opts.yang_index_schema_only:
        mods = []
        for module in modules:
            if module in mods:
                continue
            mods.append(module)
            for i in module.search('include'):
                subm = ctx.get_module(i.arg)
                if subm is None:
                    r = module.search_one('revision')
                    if r is not None:
                        subm = ctx.search_module(module.pos, i.arg, r.arg)
                if subm is not None:
                    mods.append(subm)
        for module in mods:
            if ctx.opts.yang_index_make_module_table:
                index_mprinter(ctx, module)
            non_chs = module.i_typedefs.values() + module.i_features.values() + module.i_identities.values() + \
                module.i_groupings.values() + module.i_extensions.values()
            for augment in module.search('augment'):
                if (hasattr(augment.i_target_node, 'i_module') and
                        augment.i_target_node.i_module not in mods):
                    for child in augment.i_children:
                        statements.iterate_i_children(child, index_printer)
            for nch in non_chs:
                index_printer(nch)
            for child in module.i_children:
                statements.iterate_i_children(child, index_printer)


def index_mprinter(ctx, module):
    global _yang_catalog_index_fd

    params = [module.arg]
    args = ['revision', 'yang-version', 'belongs-to',
            'namespace', 'prefix', 'organization']
    # Allow for changes to the params array wihtout needing to remember to
    # adjust static index numbers.
    bt_idx = args.index('belongs-to') + 1
    ns_idx = args.index('namespace') + 1
    org_idx = args.index('organization') + 1
    rev_idx = args.index('revision') + 1
    prefix_idx = args.index('prefix') + 1
    ver_idx = args.index('yang-version') + 1
    for a in args:
        nlist = module.search(a)
        nstr = ''
        if nlist:
            nstr = nlist[0].arg
            nstr = nstr.replace("'", r"''")
            params.append(nstr)
        else:
            params.append('')
    # Attempt to normalize the organization for catalog retrieval.
    if params[bt_idx] is not None and params[bt_idx] != '':
        bt = module.search_one('belongs-to')
        pf = bt.search_one('prefix')
        if pf is not None:
            params[prefix_idx] = pf.arg
        pm = ctx.get_module(params[bt_idx], params[rev_idx])
        if pm is None:
            pm = ctx.search_module(module.pos, params[bt_idx], params[rev_idx])
        if pm is not None:
            ns = pm.search_one('namespace')
            if ns is not None:
                params[ns_idx] = ns.arg
    m = re.search(r"urn:([^:]+):", params[ns_idx])
    if m:
        params[org_idx] = m.group(1)

    if params[ver_idx] is None or params[ver_idx] == '' or params[ver_idx] == '1':
        params[ver_idx] = '1.0'
    # We don't yet know the maturity of the module, but we can get that from
    # the catalog later.
    # The DB columns below need to be in the same order as the args list above.
    _yang_catalog_index_fd.write(
        "insert into modules (module, revision, yang_version, belongs_to, namespace, prefix, organization) values('%s', '%s', '%s', '%s', '%s', '%s', '%s');" % tuple(params) + "\n")


def index_escape_json(s):
    return s.replace("\\", r"\\").replace("'", r"''").replace("\n", r"\n").replace("\t", r"\t").replace("\"", r"\"")


def flatten_keyword(k):
    if type(k) is tuple:
        k = ':'.join(map(str, k))

    return k


def index_get_other(stmt):
    a = stmt.arg
    k = flatten_keyword(stmt.keyword)
    if a:
        a = index_escape_json(a)
    else:
        a = ''
    child = {k: {'value': a, 'has_children': False}}
    child[k]['children'] = []
    for ss in stmt.substmts:
        child[k]['has_children'] = True
        child[k]['children'].append(index_get_other(ss))
    return child


def index_printer(stmt):
    global _yang_catalog_index_fd

    if stmt.arg is None:
        return

    skey = flatten_keyword(stmt.keyword)

    module = stmt.i_module
    rev = module.search_one('revision')
    revision = ''
    if rev:
        revision = rev.arg
    path = statements.mk_path_str(stmt, True)
    descr = stmt.search_one('description')
    dstr = ''
    if descr:
        dstr = descr.arg
        dstr = dstr.replace("'", r"''")
    subs = []
    for i in stmt.substmts:
        a = i.arg
        k = i.keyword

        k = flatten_keyword(k)

        if i.keyword not in statements.data_definition_keywords:
            subs.append(index_get_other(i))
        else:
            has_children = hasattr(i, 'i_children') and len(i.i_children) > 0
            if not a:
                a = ''
            else:
                a = index_escape_json(a)
            subs.append(
                {k: {'value': a, 'has_children': has_children, 'children': []}})
    _yang_catalog_index_fd.write("insert into yindex values('%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (
        module.arg, revision, path, skey, stmt.arg, dstr, json.dumps(subs)) + "\n")
