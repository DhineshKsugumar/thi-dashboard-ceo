/**
 * THI CEO Overview Dashboard - Zoho CRM API Integration
 * Fetches live data from Leads, Deals, Events modules
 */

(function () {
  'use strict';

  var LOG = '[THI CEO]';
  function log() { console.log.apply(console, [LOG].concat(Array.prototype.slice.call(arguments))); }

  let zohoSDKReady = false;
  let kpiChart = null;
  let capacityChart = null;

  // Status mapping: Meeting_Stage -> status filter
  const STAGE_TO_STATUS = {
    'Contract Signed': 'Closed',
    'Full Demo Financial TD': 'Contacted',
    'Full Demo No Sale': 'Contacted',
    'Meeting Acknowledged': 'Contacted',
    'Issued Appointment': 'Initially',
    'Customer no show': 'No Show',
    'Cancelled - Rescheduled': 'Cancelled',
    'Cancelled - Not Rescheduled': 'Cancelled',
    'Cancelled because unconfirmed': 'Cancelled',
    'ID - Not Interested': 'Cancelled',
    'ID - Ran OutofTime': 'Cancelled',
    'DNC': 'Cancelled',
    'No Rep Available': 'Cancelled',
    'One Leg': 'Contacted',
    'Can Save': 'Contacted',
    'None': 'Initially'
  };

  function getCSTOffset() {
    try {
      const d = new Date();
      const formatter = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Chicago', timeZoneName: 'shortOffset' });
      const parts = formatter.formatToParts(d);
      const tzPart = parts.find(p => p.type === 'timeZoneName');
      const str = (tzPart && tzPart.value) || '';
      const match = str.match(/UTC([+\-\u2212])(\d{1,2}):?(\d{2})?/);
      if (match) {
        const sign = match[1] === '\u2212' ? '-' : match[1];
        const hours = String(Math.abs(parseInt(match[2], 10))).padStart(2, '0');
        const mins = (match[3] ? String(Math.abs(parseInt(match[3], 10))).padStart(2, '0') : '00');
        return sign + hours + ':' + mins;
      }
    } catch (e) {}
    return '-06:00';
  }

  function getDateInCST(offsetDays) {
    const d = new Date();
    d.setTime(d.getTime() + offsetDays * 86400000);
    const y = d.toLocaleString('en-US', { timeZone: 'America/Chicago', year: 'numeric' });
    const m = d.toLocaleString('en-US', { timeZone: 'America/Chicago', month: '2-digit' });
    const day = d.toLocaleString('en-US', { timeZone: 'America/Chicago', day: '2-digit' });
    return y + '-' + m + '-' + day;
  }

  function getDateRange(filter) {
    const today = getDateInCST(0);
    const tz = getCSTOffset();
    let start, end;
    switch (filter) {
      case 'today':
        start = end = today;
        break;
      case 'week':
        const parts = today.split('-');
        const y = parseInt(parts[0], 10), mo = parseInt(parts[1], 10) - 1, day = parseInt(parts[2], 10);
        const utcMidnight = new Date(Date.UTC(y, mo, day, 6, 0, 0));
        const dayOfWeek = utcMidnight.getUTCDay();
        start = getDateInCST(-dayOfWeek);
        end = getDateInCST(6 - dayOfWeek);
        break;
      case 'month':
      default:
        const p = today.split('-');
        start = p[0] + '-' + p[1] + '-01';
        const lastDay = new Date(parseInt(p[0], 10), parseInt(p[1], 10), 0);
        end = p[0] + '-' + p[1] + '-' + String(lastDay.getDate()).padStart(2, '0');
        break;
    }
    return { start, end, tz };
  }

  function formatCurrency(val) {
    if (val == null || isNaN(val)) return '$0';
    const n = parseFloat(val);
    if (n >= 1000000) return '$' + (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return '$' + (n / 1000).toFixed(1) + 'K';
    return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  }

  function getInitials(name) {
    if (!name || typeof name !== 'string') return '??';
    const parts = name.trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    return name.substring(0, 2).toUpperCase();
  }

  async function coqlPaginated(selectQuery, pageSize = 200) {
    log('coqlPaginated START', 'queryPrefix:', selectQuery.substring(0, 80) + '...');
    let all = [];
    let offset = 0;
    let hasMore = true;
    while (hasMore) {
      const q = selectQuery + ' limit ' + offset + ',' + pageSize;
      log('coqlPaginated fetch offset=', offset);
      const resp = await ZOHO.CRM.API.coql({ select_query: q });
      if (resp && resp.data && resp.data.length > 0) {
        all = all.concat(resp.data);
        hasMore = (resp.info && resp.info.more_records === true) || resp.data.length === pageSize;
        log('coqlPaginated got', resp.data.length, 'records, total=', all.length, 'more=', hasMore);
      } else {
        hasMore = false;
        log('coqlPaginated no more data, resp.data length=', resp && resp.data ? resp.data.length : 0);
      }
      offset += pageSize;
      if (hasMore) await new Promise(r => setTimeout(r, 100));
    }
    log('coqlPaginated END total=', all.length);
    return all;
  }

  // Same as Hotlist: fetch ALL Deals with Meeting/Event link, then filter by date in JS
  // Try multiple field combinations - Zoho CRM field names vary by org (Owner vs Sales_Rep, Close_Date vs Closing_Date, etc.)
  async function fetchDealsAll() {
    log('fetchDealsAll START');
    if (!zohoSDKReady || !ZOHO.CRM || !ZOHO.CRM.API) {
      log('fetchDealsAll SKIP - SDK not ready');
      return [];
    }
    // COQL may require WHERE; try Owner (standard) vs Sales_Rep (custom), Closing_Date vs Close_Date
    var queries = [
      'select id,Amount,Deal_Name,Stage,Sales_Rep,Sales_Rep_2,Trainee,Close_Date,Closing_Date,Created_Time,Modified_Time,Account_Name,Meeting_ID,OLD_CRM_ID from Deals where Meeting_ID is not null',
      'select id,Amount,Deal_Name,Stage,Sales_Rep,Sales_Rep_2,Trainee,Close_Date,Created_Time,Modified_Time,Events,OLD_CRM_ID from Deals where Events is not null',
      'select id,Amount,Deal_Name,Stage,Sales_Rep,Sales_Rep_2,Trainee,Close_Date,Created_Time,Modified_Time,Meeting_ID,OLD_CRM_ID from Deals where Meeting_ID is not null',
      'select id,Amount,Deal_Name,Stage,Owner,Sales_Rep,Sales_Rep_2,Trainee,Close_Date,Created_Time,Modified_Time,OLD_CRM_ID from Deals where id is not null',
      'select id,Amount,Deal_Name,Sales_Rep,Sales_Rep_2,Trainee,Close_Date,Created_Time,Modified_Time,OLD_CRM_ID from Deals where id is not null',
      'select id,Amount,Sales_Rep,Sales_Rep_2,Trainee,Closing_Date,Created_Time,Modified_Time,OLD_CRM_ID from Deals where id is not null',
      'select id,Amount,Sales_Rep,Sales_Rep_2,Trainee,Created_Time,Modified_Time,OLD_CRM_ID from Deals where id is not null',
      'select id,Amount,Created_Time,Modified_Time from Deals where id is not null'
    ];
    for (var i = 0; i < queries.length; i++) {
      try {
        log('fetchDealsAll try query', i + 1, 'of', queries.length);
        var data = await coqlPaginated(queries[i]);
        log('fetchDealsAll SUCCESS queryIndex=', i, 'count=', data.length);
        if (data.length > 0) {
          log('fetchDealsAll sample[0]:', JSON.stringify({
            Amount: data[0].Amount,
            Close_Date: data[0].Close_Date,
            Closing_Date: data[0].Closing_Date,
            Created_Time: data[0].Created_Time,
            Modified_Time: data[0].Modified_Time
          }));
        }
        return data;
      } catch (e) {
        log('fetchDealsAll query', i + 1, 'failed:', e.message);
      }
    }
    log('fetchDealsAll all queries failed');
    return [];
  }

  function getRepName(lookup) {
    if (!lookup) return null;
    var name = typeof lookup === 'object' ? (lookup.name || lookup.id) : String(lookup);
    if (!name || (typeof name === 'string' && name.trim() === '')) return null;
    return typeof name === 'string' ? name.trim() : String(name);
  }

  function getDealReps(d) {
    var reps = [];
    var names = {};
    // Sales_Rep, Sales_Rep_2, Trainee - split amount among all involved; Owner as fallback if Sales_Rep empty
    var lookups = [d.Sales_Rep, d.Sales_Rep_2, d.Trainee];
    if (!getRepName(d.Sales_Rep)) lookups.push(d.Owner);
    lookups.forEach(function(lookup) {
      var n = getRepName(lookup);
      if (n && !names[n]) { names[n] = true; reps.push(n); }
    });
    return reps;
  }

  function parseAmount(val) {
    if (val == null) return 0;
    if (typeof val === 'number' && !isNaN(val)) return val;
    if (typeof val === 'string') {
      var p = parseFloat(val.replace(/[^0-9.-]/g, ''));
      return isNaN(p) ? 0 : p;
    }
    return 0;
  }

  function getDealDate(d) {
    var dt = d.Close_Date || d.Closing_Date || d.Modified_Time || d.Created_Time;  // Closing_Date fallback for orgs without Close_Date
    if (!dt) return null;
    if (typeof dt === 'object' && dt.date) return String(dt.date);
    var s = String(dt);
    var m = s.match(/(\d{4})-(\d{2})-(\d{2})/);
    if (m) return m[0];
    return s;
  }

  function getDealCreatedDate(d) {
    var dt = d.Created_Time;
    if (!dt) return null;
    if (typeof dt === 'object' && dt.date) return String(dt.date);
    var s = String(dt);
    var m = s.match(/(\d{4})-(\d{2})-(\d{2})/);
    if (m) return m[0];
    return s;
  }

  function isNewCRMDeal(d) {
    var v = d.OLD_CRM_ID;
    return v == null || v === '' || (typeof v === 'string' && v.trim() === '');
  }

  function dealCreatedInRange(d, start, end) {
    var dtStr = getDealCreatedDate(d);
    if (!dtStr) return false;
    var datePart = dtStr.substring(0, 10);
    return datePart >= start && datePart <= end;
  }

  function processDeals(deals, start, end) {
    log('processDeals START range=', start, 'to', end, 'totalDeals=', deals.length, '(use Created_Time, OLD_CRM_ID null/empty)');
    var totalRevenue = 0;
    var byRep = {};
    var byMeetingId = {};
    var byDay = {};
    var filtered = deals.filter(function(d) {
      return isNewCRMDeal(d) && dealCreatedInRange(d, start, end);
    });
    log('processDeals filtered count=', filtered.length);
    if (deals.length > 0 && filtered.length === 0) {
      log('processDeals WARN: no deals in range. Sample:', deals.slice(0, 3).map(function(d) {
        return { created: getDealCreatedDate(d), OLD_CRM_ID: d.OLD_CRM_ID, Amount: d.Amount };
      }));
    }
    filtered.forEach(function(d) {
      var amt = parseAmount(d.Amount);
      totalRevenue += amt;
      var reps = getDealReps(d);
      if (reps.length === 0) reps = ['Unknown'];
      var share = amt / reps.length;
      reps.forEach(function(rep) {
        byRep[rep] = (byRep[rep] || 0) + share;
      });
      var meetingLookup = d.Meeting_ID || d.Events;
      var mid = meetingLookup && (typeof meetingLookup === 'object' ? meetingLookup.id : meetingLookup);
      if (mid) byMeetingId[mid] = (byMeetingId[mid] || 0) + 1;
      var dtStr = getDealCreatedDate(d);
      if (dtStr) {
        var dayNum = parseInt(dtStr.substring(8, 10), 10);
        if (!isNaN(dayNum) && dayNum >= 1 && dayNum <= 31) byDay[dayNum] = (byDay[dayNum] || 0) + amt;
      }
    });
    log('processDeals END totalRevenue=', totalRevenue, 'count=', filtered.length, 'byDay keys=', Object.keys(byDay));
    return { totalRevenue, count: filtered.length, byRep, byMeetingId, byDay };
  }

  // Chart: Current month (projected) vs Previous month (actual cumulative)
  // Both use same filters: Created_Time in range, OLD_CRM_ID null/empty
  // Current: linear projection from revenue so far (daily rate × day)
  // Previous: cumulative sum of daily revenue (byDay = amount created each day)
  function buildChartData(dealsCurrent, dealsPrev, start, end, todayDay, daysInMonth, prevDaysInMonth) {
    log('buildChartData START todayDay=', todayDay, 'daysInMonth=', daysInMonth, 'currentRevenue=', dealsCurrent.totalRevenue, 'prevRevenue=', dealsPrev.totalRevenue);
    log('buildChartData prevMonth byDay (daily amounts):', dealsPrev.byDay ? JSON.stringify(dealsPrev.byDay) : '{}');
    var currentProjected = [];
    var prevCumulative = [];
    var labels = [];
    var prevCumul = 0;

    var totalSoFar = dealsCurrent.totalRevenue || 0;
    var dailyRate = todayDay > 0 ? totalSoFar / todayDay : 0;
    for (var d = 1; d <= daysInMonth; d++) {
      labels.push('Day ' + d);
      currentProjected.push(Math.round(d * dailyRate));
      if (d <= (prevDaysInMonth || 31)) {
        var prevDaySum = dealsPrev.byDay && dealsPrev.byDay[d] ? dealsPrev.byDay[d] : 0;
        prevCumul += prevDaySum;
      }
      prevCumulative.push(Math.round(prevCumul));
    }

    var result = {
      labels: labels,
      currentMonth: currentProjected,
      previousMonth: prevCumulative,
      currentTotal: totalSoFar,
      currentProjected: Math.round(daysInMonth * dailyRate),
      previousTotal: prevCumul
    };
    log('buildChartData END dailyRate=', dailyRate, 'currentProjected=', result.currentProjected, 'previousTotal=', result.previousTotal);
    return result;
  }

  async function fetchLeads(start, end, tz) {
    log('fetchLeads START', start, 'to', end);
    if (!zohoSDKReady || !ZOHO.CRM || !ZOHO.CRM.API) return { count: 0, bySource: {} };
    var tzStr = tz || '-06:00';
    try {
      // Zoho COQL requires ISO 8601 date format with timezone for Created_Time
      var startDt = start + 'T00:00:00' + tzStr;
      var endDt = end + 'T23:59:59' + tzStr;
      const q = `select id,Lead_Source from Leads where Created_Time >= '${startDt}' and Created_Time <= '${endDt}'`;
      const data = await coqlPaginated(q);
      const bySource = {};
      data.forEach(d => {
        const src = (d.Lead_Source || 'Direct').trim() || 'Direct';
        bySource[src] = (bySource[src] || 0) + 1;
      });
      log('fetchLeads END count=', data.length, 'bySource=', bySource);
      return { count: data.length, bySource };
    } catch (e) {
      log('fetchLeads ERROR', e.message);
      return { count: 0, bySource: {} };
    }
  }

  async function fetchEvents(start, end, tz) {
    log('fetchEvents START', start, 'to', end);
    if (!zohoSDKReady || !ZOHO.CRM || !ZOHO.CRM.API) return { count: 0, byStage: {}, byRep: {}, byBrand: {}, bySource: {}, meetingIdToBrand: {} };
    try {
      const startDt = start + 'T00:00:00' + tz;
      const endDt = end + 'T23:59:59' + tz;
      const q = `select id,Meeting_Stage,Sales_Rep,Amount,Brand,Lead_Source from Events where Start_DateTime between '${startDt}' and '${endDt}'`;
      const data = await coqlPaginated(q);
      const byStage = {};
      const byRep = {};
      const byBrand = {};
      const bySource = { Marketing: 0, Partner: 0 };
      const meetingIdToBrand = {};
      data.forEach(d => {
        const stage = (d.Meeting_Stage || 'None').trim();
        byStage[stage] = (byStage[stage] || 0) + 1;
        const rep = (d.Sales_Rep && (typeof d.Sales_Rep === 'object' ? d.Sales_Rep.name : d.Sales_Rep)) || 'Unknown';
        const amt = parseFloat(d.Amount || 0) || 0;
        byRep[rep] = (byRep[rep] || 0) + amt;
        const brand = (d.Brand && (typeof d.Brand === 'object' ? d.Brand.name : d.Brand)) || 'Other';
        byBrand[brand] = byBrand[brand] || { leads: 0, meetings: 0, deals: 0 };
        byBrand[brand].meetings++;
        if (d.id) meetingIdToBrand[d.id] = brand;
        const src = (d.Lead_Source || '').toLowerCase();
        if (src.includes('marketing') || src.includes('digital')) bySource.Marketing++;
        else if (src.includes('partner') || src.includes('canvassing')) bySource.Partner++;
      });
      log('fetchEvents END count=', data.length, 'byStage keys=', Object.keys(byStage));
      return { count: data.length, byStage, byRep, byBrand, bySource, meetingIdToBrand };
    } catch (e) {
      log('fetchEvents ERROR', e.message);
      return { count: 0, byStage: {}, byRep: {}, byBrand: {}, bySource: {}, meetingIdToBrand: {} };
    }
  }


  function mapStageToStatus(stage) {
    return STAGE_TO_STATUS[stage] || 'Initially';
  }

  function aggregateStatusCounts(byStage) {
    const statuses = { Closed: 0, Contacted: 0, Cancelled: 0, 'No Show': 0, Initially: 0 };
    Object.keys(byStage || {}).forEach(stage => {
      const status = mapStageToStatus(stage);
      if (statuses[status] !== undefined) statuses[status] += byStage[stage];
    });
    return statuses;
  }

  function addLiveBadge(container, isLive) {
    if (!container) return;
    let badge = container.querySelector('.live-badge');
    if (isLive) {
      if (!badge) {
        badge = document.createElement('span');
        badge.className = 'live-badge';
        badge.textContent = 'LIVE';
        badge.title = 'Connected to real-time Zoho CRM data';
        container.style.position = 'relative';
        container.appendChild(badge);
      }
    } else if (badge) {
      badge.remove();
    }
  }

  let lastLoadedData = null;

  window.CEO_API = {
    init: async function () {
      log('init START');
      if (typeof ZOHO === 'undefined' || !ZOHO.CRM || !ZOHO.CRM.API) {
        log('init FAIL - ZOHO.CRM.API not available');
        return false;
      }
      zohoSDKReady = true;
      log('init OK');
      return true;
    },

    loadAll: async function (dateFilter) {
      log('loadAll START filter=', dateFilter);
      const filter = dateFilter || 'month';
      const { start, end, tz } = getDateRange(filter);
      log('loadAll dateRange', start, 'to', end, 'tz=', tz);

      // Previous period for comparison
      let prevStart, prevEnd;
      if (filter === 'month') {
        const d = new Date(start);
        d.setMonth(d.getMonth() - 1);
        prevStart = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-01';
        const lastD = new Date(d.getFullYear(), d.getMonth() + 1, 0);
        prevEnd = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(lastD.getDate()).padStart(2, '0');
      } else {
        const days = (new Date(end) - new Date(start)) / 86400000 + 1;
        prevEnd = start;
        const p = new Date(start);
        p.setDate(p.getDate() - days);
        prevStart = p.toISOString().slice(0, 10);
      }

      var allDeals = await fetchDealsAll();
      log('loadAll allDeals count=', allDeals.length);
      var deals = processDeals(allDeals, start, end);
      var prevDeals = processDeals(allDeals, prevStart, prevEnd);
      log('loadAll current deals revenue=', deals.totalRevenue, 'count=', deals.count);
      log('loadAll prev deals revenue=', prevDeals.totalRevenue, 'count=', prevDeals.count);

      // Top section: Revenue uses date range; Leads/Meetings/Contracts use TODAY vs YESTERDAY
      var todayStr = getDateInCST(0);
      var yesterdayStr = getDateInCST(-1);
      const [leads, prevLeads, events, prevEvents, todayLeads, yesterdayLeads, todayEvents, yesterdayEvents] = await Promise.all([
        fetchLeads(start, end, tz),
        fetchLeads(prevStart, prevEnd, tz),
        fetchEvents(start, end, tz),
        fetchEvents(prevStart, prevEnd, tz),
        fetchLeads(todayStr, todayStr, tz),
        fetchLeads(yesterdayStr, yesterdayStr, tz),
        fetchEvents(todayStr, todayStr, tz),
        fetchEvents(yesterdayStr, yesterdayStr, tz)
      ]);
      var todayContractsCount = allDeals.filter(function(d) {
        var created = getDealCreatedDate(d);
        return isNewCRMDeal(d) && created && created.substring(0, 10) === todayStr;
      }).length;
      var yesterdayContractsCount = allDeals.filter(function(d) {
        var created = getDealCreatedDate(d);
        return isNewCRMDeal(d) && created && created.substring(0, 10) === yesterdayStr;
      }).length;
      log('loadAll today metrics: leads=', todayLeads.count, 'meetings=', todayEvents.count, 'contracts=', todayContractsCount);
      log('loadAll yesterday: leads=', yesterdayLeads.count, 'meetings=', yesterdayEvents.count, 'contracts=', yesterdayContractsCount);

      var chartData = null;
      if (filter === 'month') {
        var todayParts = getDateInCST(0).split('-');
        var todayDay = parseInt(todayParts[2], 10) || 1;
        var daysInMonth = new Date(parseInt(todayParts[0], 10), parseInt(todayParts[1], 10), 0).getDate();
        var prevDaysInMonth = new Date(parseInt(todayParts[0], 10), parseInt(todayParts[1], 10) - 1, 0).getDate();
        chartData = buildChartData(deals, prevDeals, start, end, todayDay, daysInMonth, prevDaysInMonth);
      }

      // Merge deals into brand performance via Meeting_ID -> Event Brand
      const brandPerf = {};
      Object.keys(events.byBrand || {}).forEach(brand => {
        brandPerf[brand] = { ...events.byBrand[brand], deals: 0 };
      });
      const eventBrandMap = events.meetingIdToBrand || {};
      Object.entries(deals.byMeetingId || {}).forEach(([meetingId, dealCount]) => {
        const brand = eventBrandMap[meetingId] || 'Other';
        brandPerf[brand] = brandPerf[brand] || { leads: 0, meetings: 0, deals: 0 };
        brandPerf[brand].deals = (brandPerf[brand].deals || 0) + dealCount;
      });

      const dealCount = deals.count;
      const prevDealCount = prevDeals.count;
      const revenue = deals.totalRevenue;
      const prevRevenue = prevDeals.totalRevenue;
      const revPct = prevRevenue > 0 ? ((revenue - prevRevenue) / prevRevenue * 100).toFixed(0) : 0;
      // Top metrics: vs yesterday comparison
      const leadPct = yesterdayLeads.count > 0 ? ((todayLeads.count - yesterdayLeads.count) / yesterdayLeads.count * 100).toFixed(0) : (todayLeads.count > 0 ? '100' : '0');
      const meetingPct = yesterdayEvents.count > 0 ? ((todayEvents.count - yesterdayEvents.count) / yesterdayEvents.count * 100).toFixed(0) : (todayEvents.count > 0 ? '100' : '0');
      const dealPct = yesterdayContractsCount > 0 ? ((todayContractsCount - yesterdayContractsCount) / yesterdayContractsCount * 100).toFixed(0) : (todayContractsCount > 0 ? '100' : '0');

      // Sales Rep Ranking: same data as Revenue card (deals only, Created_Time in range, OLD_CRM_ID null/empty)
      const repRevenue = { ...deals.byRep };

      const leaderboard = Object.entries(repRevenue)
        .filter(([, v]) => v > 0)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([name, rev], i) => ({
          rank: i + 1,
          name,
          initials: getInitials(name),
          revenue: rev,
          pct: 100
        }));

      const maxRev = leaderboard[0] ? leaderboard[0].revenue : 1;
      leaderboard.forEach(r => { r.pct = Math.round((r.revenue / maxRev) * 100); });

      const statusCounts = aggregateStatusCounts(events.byStage);

      const marketingLeads = (leads.bySource['Marketing'] || 0) + (leads.bySource['Digital'] || 0);
      const partnerLeads = (leads.bySource['Partner'] || 0) + (leads.bySource['Canvassing'] || 0);

      // Capacity: Canvassing vs Digital (from leads in selected period)
      var canvassingCount = 0;
      Object.keys(leads.bySource || {}).forEach(function(src) {
        if (src && String(src).toLowerCase().indexOf('canvassing') >= 0) canvassingCount += leads.bySource[src];
      });
      var totalLeads = leads.count || 0;
      var digitalCount = Math.max(0, totalLeads - canvassingCount);
      var canvassingPct = totalLeads > 0 ? Math.round(canvassingCount / totalLeads * 100) : 50;
      var digitalPct = totalLeads > 0 ? Math.round(digitalCount / totalLeads * 100) : 50;
      if (canvassingPct + digitalPct !== 100 && totalLeads > 0) digitalPct = 100 - canvassingPct;
      log('loadAll capacity: canvassing=', canvassingCount, 'digital=', digitalCount, 'canvassingPct=', canvassingPct);

      log('loadAll FINAL metrics revenue=', revenue, 'leadsToday=', todayLeads.count, 'meetingsToday=', todayEvents.count, 'contractsToday=', todayContractsCount);
      log('loadAll returning');

      return {
        metrics: {
          revenue,
          prevRevenue,
          revPct,
          leads: todayLeads.count,
          prevLeads: yesterdayLeads.count,
          leadPct,
          meetings: todayEvents.count,
          prevMeetings: yesterdayEvents.count,
          meetingPct,
          deals: todayContractsCount,
          prevDeals: yesterdayContractsCount,
          dealPct
        },
        leaderboard,
        statusCounts,
        marketingSummary: {
          marketingLeads,
          partnerLeads,
          meetings: events.count,
          deals: dealCount
        },
        brandPerformance: brandPerf,
        dateRange: { start, end },
        chartData: chartData,
        capacity: { canvassing: canvassingCount, digital: digitalCount, total: totalLeads, canvassingPct, digitalPct }
      };
    },

    render: function (data) {
      log('render START');
      lastLoadedData = data;
      const m = data.metrics;
      log('render metrics', m);
      const setText = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
      };
      const setTrend = (id, pct, isUp, vsText) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = (isUp ? '↑ ' : '↓ ') + Math.abs(parseFloat(pct)) + '% vs ' + (vsText || 'last period');
        el.className = 'metric-trend ' + (isUp ? 'up' : 'down');
      };
      setText('metricRevenue', formatCurrency(m.revenue));
      setTrend('metricRevenueTrend', m.revPct, m.revenue >= m.prevRevenue);
      setText('metricLeads', m.leads);
      setTrend('metricLeadsTrend', m.leadPct, m.leads >= m.prevLeads, 'yesterday');
      setText('metricMeetings', m.meetings);
      setTrend('metricMeetingsTrend', m.meetingPct, m.meetings >= m.prevMeetings, 'yesterday');
      setText('metricDeals', m.deals);
      setTrend('metricDealsTrend', m.dealPct, m.deals >= m.prevDeals, 'yesterday');

      // Status cards
      const statusIds = { Closed: 'statusClosed', Contacted: 'statusContacted', Cancelled: 'statusCancelled', 'No Show': 'statusNoShow', Initially: 'statusInitially' };
      Object.entries(data.statusCounts).forEach(([status, count]) => {
        const id = statusIds[status];
        if (id) setText(id, count);
      });

      // Leaderboard
      const listEl = document.getElementById('leaderboardList');
      if (listEl) {
        listEl.innerHTML = data.leaderboard.map((r, i) => `
          <div class="leaderboard-item ${i === 0 ? 'top' : ''}">
            <span class="leaderboard-rank">${r.rank}</span>
            <div class="leaderboard-avatar">${r.initials}</div>
            <div class="leaderboard-info">
              <div class="leaderboard-name">${r.name}</div>
              <div class="leaderboard-value">${formatCurrency(r.revenue)} revenue</div>
              <div class="leaderboard-progress"><div class="leaderboard-progress-fill" style="width:${r.pct}%"></div></div>
            </div>
            <span class="leaderboard-trend up">↑</span>
          </div>
        `).join('');
      }

      // Marketing summary
      const ms = data.marketingSummary;
      setText('marketingLeadsVal', ms.marketingLeads);
      setText('partnerLeadsVal', ms.partnerLeads);
      setText('marketingMeetingsVal', ms.meetings);
      setText('marketingDealsVal', ms.deals);

      // Brand performance table
      const brandTbody = document.getElementById('brandTableBody');
      if (brandTbody && data.brandPerformance) {
        const rows = Object.entries(data.brandPerformance).map(([brand, stats]) =>
          `<tr><td>${brand}</td><td>${stats.leads || 0}</td><td>${stats.meetings || 0}</td><td>${stats.deals || 0}</td></tr>`
        ).join('');
        brandTbody.innerHTML = rows || '<tr><td colspan="4">No data</td></tr>';
      }

      // Chart
      if (data.chartData && kpiChart) {
        kpiChart.data.labels = data.chartData.labels;
        kpiChart.data.datasets[0].data = data.chartData.currentMonth;
        kpiChart.data.datasets[0].label = 'Current month (projected from contracts)';
        kpiChart.data.datasets[1].data = data.chartData.previousMonth;
        kpiChart.data.datasets[1].label = 'Previous month (contract revenue from Deals)';
        kpiChart.update();
      }

      // Chart headline
      var chartHeadline = document.getElementById('chartHeadline');
      if (chartHeadline && data.chartData) {
        chartHeadline.textContent = 'Current month: projected to $' + (data.chartData.currentProjected || 0).toLocaleString() + ' | Previous month: $' + (data.chartData.previousTotal || 0).toLocaleString() + ' from Deals';
      }

      // Capacity chart: Canvassing vs Digital
      if (capacityChart && data.capacity) {
        var cap = data.capacity;
        var canv = cap.canvassing || 0;
        var dig = cap.digital || 0;
        if (canv === 0 && dig === 0) { canv = 1; dig = 1; }
        capacityChart.data.labels = ['Canvassing', 'Digital'];
        capacityChart.data.datasets[0].data = [canv, dig];
        capacityChart.data.datasets[0].backgroundColor = ['#8A5CF6', '#4F8CFF'];
        capacityChart.update();
      }
      var capVal = document.getElementById('capacityValue');
      if (capVal && data.capacity) {
        var total = data.capacity.total || 0;
        var canvPct = data.capacity.canvassingPct || 0;
        var digPct = data.capacity.digitalPct || 0;
        if (total > 0) {
          capVal.textContent = canvPct >= digPct ? canvPct + '% Canvassing' : digPct + '% Digital';
        } else {
          capVal.textContent = '—';
        }
      }

      // LIVE badges
      document.querySelectorAll('.metric-card').forEach(function(card) { addLiveBadge(card, true); });
      addLiveBadge(document.querySelector('.sidebar-card'), true);
      addLiveBadge(document.querySelector('.filters-section'), true);
      addLiveBadge(document.querySelector('.graph-card'), true);
      var mGrid = document.querySelector('.marketing-cards-grid');
      addLiveBadge(mGrid ? mGrid.closest('.table-card') : null, true);
      var brandTbl = document.getElementById('brandTableBody');
      addLiveBadge(brandTbl ? brandTbl.closest('.table-card') : null, true);
      log('render END');
    },

    setCharts: function (kpi, capacity) {
      log('setCharts kpi=', !!kpi, 'capacity=', !!capacity);
      kpiChart = kpi;
      capacityChart = capacity;
    },

    updateChartData: function (dailyData) {
      if (kpiChart && dailyData) {
        kpiChart.data.datasets[0].data = dailyData.current;
        kpiChart.data.datasets[1].data = dailyData.previous;
        kpiChart.update();
      }
    },

    exportMetrics: function () {
      log('exportMetrics START');
      var data = lastLoadedData;
      log('exportMetrics lastLoadedData=', !!data);
      var kv = {};
      if (data) {
        kv['Date Range'] = (data.dateRange.start || '') + ' to ' + (data.dateRange.end || '');
        kv['Revenue'] = data.metrics.revenue;
        kv['Revenue Trend %'] = data.metrics.revPct;
        kv['Leads Today'] = data.metrics.leads;
        kv['Meetings Today'] = data.metrics.meetings;
        kv['Contracts Today'] = data.metrics.deals;
        kv['Status - Closed'] = data.statusCounts.Closed || 0;
        kv['Status - Contacted'] = data.statusCounts.Contacted || 0;
        kv['Status - Cancelled'] = data.statusCounts.Cancelled || 0;
        kv['Status - No Show'] = data.statusCounts['No Show'] || 0;
        kv['Status - Initially'] = data.statusCounts.Initially || 0;
        kv['Marketing Leads'] = data.marketingSummary.marketingLeads || 0;
        kv['Partner Leads'] = data.marketingSummary.partnerLeads || 0;
        kv['Marketing Meetings'] = data.marketingSummary.meetings || 0;
        kv['Marketing Deals'] = data.marketingSummary.deals || 0;
        if (data.capacity) {
          kv['Capacity - Canvassing'] = data.capacity.canvassing;
          kv['Capacity - Digital'] = data.capacity.digital;
          kv['Capacity - Canvassing %'] = data.capacity.canvassingPct + '%';
        }
        (data.leaderboard || []).forEach(function (r, i) {
          kv['Sales Rep #' + (i + 1)] = r.name + ': $' + (r.revenue || 0).toLocaleString();
        });
        Object.keys(data.brandPerformance || {}).forEach(function (brand) {
          var b = data.brandPerformance[brand];
          kv['Brand ' + brand + ' - Leads'] = b.leads || 0;
          kv['Brand ' + brand + ' - Meetings'] = b.meetings || 0;
          kv['Brand ' + brand + ' - Deals'] = b.deals || 0;
        });
      } else {
        kv['Revenue'] = document.getElementById('metricRevenue') ? document.getElementById('metricRevenue').textContent : '';
        kv['Leads Today'] = document.getElementById('metricLeads') ? document.getElementById('metricLeads').textContent : '';
        kv['Meetings Today'] = document.getElementById('metricMeetings') ? document.getElementById('metricMeetings').textContent : '';
        kv['Contracts Today'] = document.getElementById('metricDeals') ? document.getElementById('metricDeals').textContent : '';
        kv['Status - Closed'] = document.getElementById('statusClosed') ? document.getElementById('statusClosed').textContent : '';
        kv['Status - Contacted'] = document.getElementById('statusContacted') ? document.getElementById('statusContacted').textContent : '';
        kv['Status - Cancelled'] = document.getElementById('statusCancelled') ? document.getElementById('statusCancelled').textContent : '';
        kv['Status - No Show'] = document.getElementById('statusNoShow') ? document.getElementById('statusNoShow').textContent : '';
        kv['Status - Initially'] = document.getElementById('statusInitially') ? document.getElementById('statusInitially').textContent : '';
        kv['Marketing Leads'] = document.getElementById('marketingLeadsVal') ? document.getElementById('marketingLeadsVal').textContent : '';
        kv['Partner Leads'] = document.getElementById('partnerLeadsVal') ? document.getElementById('partnerLeadsVal').textContent : '';
        kv['Meetings'] = document.getElementById('marketingMeetingsVal') ? document.getElementById('marketingMeetingsVal').textContent : '';
        kv['Deals'] = document.getElementById('marketingDealsVal') ? document.getElementById('marketingDealsVal').textContent : '';
      }
      var json = JSON.stringify(kv, null, 2);
      var blob = new Blob([json], { type: 'application/json' });
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = 'THI_CEO_Metrics_' + new Date().toISOString().slice(0, 10) + '.json';
      a.click();
      URL.revokeObjectURL(url);
      log('exportMetrics END downloaded');
    }
  };
})();
