#ifndef CACHE_H
#define CACHE_H

// LRU cache structures
void cache_init();

int cache_serve(const char *query, char *out_buffer); 
void cache_put(const char *query, const char *result);

#endif
