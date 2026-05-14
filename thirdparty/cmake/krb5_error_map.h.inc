/*
 * This file is generated, please don't edit it.
 * script: /mnt/disk7/sunchenyang/doris/thirdparty/src/krb5-1.19/src/util/gen-map.pl
 * args:
 *	-oerror_map.h
 *	NAME=gsserrmap
 *	KEY=OM_uint32
 *	VALUE=char *
 *	COMPARE=compare_OM_uint32
 * The rest of this file is copied from a template, with
 * substitutions.  See the template for copyright info.
 */
/*
 * map, generated from template
 * map name: gsserrmap
 * key: OM_uint32
 * value: char *
 * compare: compare_OM_uint32
 * copy_key: 0
 * free_key: 0
 * free_value: 0
 */
struct gsserrmap__element {
    OM_uint32 key;
    char * value;
    struct gsserrmap__element *next;
};
struct gsserrmap__head {
    struct gsserrmap__element *first;
};
typedef struct gsserrmap__head gsserrmap;
static inline int gsserrmap_init (struct gsserrmap__head *head)
{
    head->first = NULL;
    return 0;
}
static inline void gsserrmap_destroy (struct gsserrmap__head *head)
{
    struct gsserrmap__element *e, *e_next;
    void (*free_key)(OM_uint32) = 0;
    void (*free_value)(char *) = 0;
    for (e = head->first; e; e = e_next) {
	e_next = e->next;
	if (free_key)
	    (*free_key)(e->key);
	if (free_value)
	    (*free_value)(e->value);
	free(e);
    }
    head->first = NULL;
}
/* Returns pointer to linked-list entry, or null if key not found.  */
static inline struct gsserrmap__element *
gsserrmap__find_node (struct gsserrmap__head *head, OM_uint32 key)
{
    struct gsserrmap__element *e;
    for (e = head->first; e; e = e->next)
	if (compare_OM_uint32 (key, e->key) == 0)
	    return e;
    return 0;
}
/* Returns pointer to value, or null if key not found.  */
static inline char * *
gsserrmap_find (struct gsserrmap__head *head, OM_uint32 key)
{
    struct gsserrmap__element *e = gsserrmap__find_node(head, key);
    if (e)
	return &e->value;
    return 0;
}
/* Returns 0 or error code.  */
static inline int
gsserrmap__copy_key (OM_uint32 *out, OM_uint32 in)
{
    int (*copykey)(OM_uint32 *, OM_uint32) = 0;
    if (copykey == 0) {
	*out = in;
	return 0;
    } else
	return (*copykey)(out, in);
}
/* Returns 0 or error code.  */
static inline int
gsserrmap_replace_or_insert (struct gsserrmap__head *head,
			  OM_uint32 key, char * new_value)
{
    struct gsserrmap__element *e = gsserrmap__find_node(head, key);
    int ret;

    if (e) {
	/* replace */
	void (*free_value)(char *) = 0;
	if (free_value)
	    (*free_value)(e->value);
	e->value = new_value;
    } else {
	/* insert */
	e = malloc(sizeof(*e));
	if (e == NULL)
	    return ENOMEM;
	ret = gsserrmap__copy_key (&e->key, key);
	if (ret) {
	    free(e);
	    return ret;
	}
	e->value = new_value;
	e->next = head->first;
	head->first = e;
    }
    return 0;
}
