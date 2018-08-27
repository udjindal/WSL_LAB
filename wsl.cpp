#include "bits/stdc++.h"
using namespace std;

typedef int i;
typedef long int l;
typedef long long int ll;
typedef float f;
typedef long double lf;
typedef char c;
const int inf = INT_MAX, mod = 1e7 + 9;
const int MAX = 1e7;
#define FASTER ios_base::sync_with_stdio(false); cin.tie(NULL); cout.tie(NULL)
#define pb push_back
#define mp make_pair
int main(void) {
    FASTER;
    lf c[] = {2/8, 4/8, 3/8, 1, 7/8};
    lf a[] = {3/9, 2/9, 5/9, 7/9, 1};

    map <int, vector<int> > end_by;
    end_by[0].pb(3); end_by[0].pb(4);

    end_by[1].pb(2); end_by[1].pb(4); end_by[1].pb(0);

    end_by[2].pb(1); end_by[2].pb(3); end_by[2].pb(4);

    end_by[3].pb(4);

    end_by[4].pb(1); end_by[4].pb(3);

    map <int, vector<int> > end_to;
    end_to[0].pb(1);

    end_to[1].pb(2); end_to[1].pb(4);

    end_to[2].pb(1);

    end_to[3].pb(0); end_to[3].pb(2); end_to[3].pb(4);

    end_to[4].pb(0); end_to[4].pb(1); end_to[4].pb(3);
    int cnt = 0;
    lf maxi = INT_MIN;
    while(cnt < 20) {
        lf a_n[5];
        lf c_n[5];
        lf max_a = INT_MIN;
        lf max_c = INT_MIN;
        for(int i = 0; i < 5; i++) {
            a_n[i] = a[i];
            for(int j = 0; j < end_by[i].size(); j++) {
                a_n[i] += c[end_by[i][j]];
            }
            max_a = max(max_a, a_n[i]);
        }
        for(int i = 0; i < 5; i++) {
            a_n[i] /= max_a;
        }

        for(int i = 0; i < 5; i++) {
            c_n[i] = c[i];
            for(int j = 0; j < end_to[i].size(); j++) {
                c_n[i] += a[end_to[i][j]];
            }
            max_c = max(max_c, c_n[i]);
        }
        for(int i = 0; i < 5; i++) {
            c_n[i] /= max_c;
        }

        lf max_change_a = INT_MIN;
        lf max_change_c = INT_MIN;
        for(int i = 0; i < 5; i++) {
            max_change_a = max(max_change_a, abs(a_n[i] - a[i]));
            max_change_c = max(max_change_c, abs(c_n[i] - c[i]));
            a[i] = a_n[i];
            c[i] = c_n[i];
        }
        maxi = max(max_change_a, max_change_c);
        cout << "change occured in this loop "<< maxi << endl;
        cnt++;
    }


    return 0;
}
